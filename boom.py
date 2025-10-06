"""
CDN 流量触发器（改进版）

功能要点（本版本修复并强化）：
- 使用生产者-消费者（asyncio.Queue）模型，避免一次性创建大量协程导致 session 被关闭或内存暴涨。
- 全局唯一 aiohttp.ClientSession，在整个运行周期内保持打开，确保不会出现 "Session is closed"。
- 可控并发：由 worker 数量决定（并发与连接数分离）。
- 速率控制：基于令牌桶(Token Bucket)或每次 sleep，两种模式可选。
- 支持代理池（按请求随机选取代理，支持 http/https 代理字符串）。
- 支持 cache-bust、是否读取响应体(read_response)、禁用/启用 SSL 校验。
- 支持按持续时间或总请求数停止；优雅停止并统计结果。
- GUI (Tkinter) 控件集成，允许实时查看日志和统计。

使用说明：
1. 仅用于你拥有或被授权的域名/资源，滥用可能违法。
2. 依赖：pip install aiohttp aiofiles
3. 运行：python cdn_traffic_trigger_gui.py

"""
import webbrowser
import asyncio
import aiohttp
import time
import random
import string
import threading
import sys
import os
from urllib.parse import urlparse
import tkinter as tk
from tkinter import scrolledtext, filedialog, messagebox
from tkinter import ttk

# ---------------------- 工具函数 ----------------------

def random_suffix(n=8):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=n))

def parse_proxies(text):
    proxies = []
    for line in text.splitlines():
        s = line.strip()
        if not s:
            continue
        if s.startswith('http://') or s.startswith('https://'):
            proxies.append(s)
        else:
            proxies.append('http://' + s)
    return proxies

def parse_urls(text):
    urls = []
    for line in text.splitlines():
        s = line.strip()
        if not s:
            continue
        urls.append(s)
    return urls

# ---------------------- Trigger 核心（队列+worker 模型） ----------------------
class RobustCDNTrigger:
    def __init__(self, ui_callback=None):
        self._stop = threading.Event()
        self._thread = None
        self._ui_callback = ui_callback
        self.stats = {'sent':0,'success':0,'failed':0,'bytes':0,'start_time':None}

    def start(self, config):
        if self._thread and self._thread.is_alive():
            raise RuntimeError('Already running')
        self._stop.clear()
        self.config = config
        self._thread = threading.Thread(target=self._run_thread, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()

    def _push_log(self, msg):
        if self._ui_callback:
            try:
                self._ui_callback('log', msg)
            except Exception:
                pass

    def _push_stats(self):
        if self._ui_callback:
            try:
                self._ui_callback('stat', dict(self.stats))
            except Exception:
                pass

    def _run_thread(self):
        try:
            asyncio.run(self._main())
        except Exception as e:
            self._push_log(f'主程序异常: {e}')

    async def _worker(self, name, session, queue, rate_limiter):
        """消费者：从 queue 中取出 URL 并发请求"""
        while not self._stop.is_set():
            try:
                item = await asyncio.wait_for(queue.get(), timeout=0.5)
            except asyncio.TimeoutError:
                # 若队列空且生产者已完成则退出
                if getattr(self, '_producer_done', False):
                    break
                continue

            url, idx = item
            try:
                # rate control
                if rate_limiter is not None:
                    await rate_limiter.get()

                if self.config['cache_bust']:
                    sep = '&' if '?' in url else '?'
                    url_req = f"{url}{sep}v={int(time.time()*1000)}_{random_suffix(6)}"
                else:
                    url_req = url

                headers = {'User-Agent': random.choice(self.config['user_agents']),
                           'Accept':'*/*','Cache-Control':'no-cache','Pragma':'no-cache'}

                proxy = None
                if self.config['proxies']:
                    proxy = random.choice(self.config['proxies'])

                timeout = aiohttp.ClientTimeout(total=self.config['req_timeout'])
                async with session.get(url_req, headers=headers, timeout=timeout, proxy=proxy) as resp:
                    data = await resp.content.read() if self.config['read_response'] else b''
                    self.stats['sent'] += 1
                    if 200 <= resp.status < 300:
                        self.stats['success'] += 1
                        self.stats['bytes'] += len(data)
                        self._push_log(f"[{idx}] {resp.status} {url} ({len(data)} B)")
                    else:
                        self.stats['failed'] += 1
                        self._push_log(f"[{idx}] HTTP {resp.status} {url}")
            except Exception as e:
                self.stats['sent'] += 1
                self.stats['failed'] += 1
                self._push_log(f"[{idx}] 异常: {e}")
            finally:
                queue.task_done()
                # push stats occasionally
                if self.stats['sent'] % 5 == 0:
                    self._push_stats()

    async def _producer(self, queue):
        """生产者：根据 total 或 duration 将任务放入队列"""
        total = int(self.config.get('total',0))
        duration = int(self.config.get('duration',0))
        urls = list(self.config['urls'])
        t0 = time.time()
        i = 0
        while not self._stop.is_set():
            if duration > 0 and (time.time()-t0) >= duration:
                break
            if total > 0 and i >= total:
                break
            i += 1
            url = random.choice(urls)
            await queue.put((url, i))
            # optional small sleep to avoid瞬时爆发到 queue
            if self.config.get('prod_sleep'):
                await asyncio.sleep(self.config['prod_sleep'])
        # producer done
        self._producer_done = True

    async def _refiller(self, rate, token_q):
        if rate <= 0:
            return
        interval = 1.0
        while not self._stop.is_set():
            for _ in range(rate):
                try:
                    token_q.put_nowait(1)
                except asyncio.QueueFull:
                    pass
            await asyncio.sleep(interval)

    async def _main(self):
        cfg = self.config
        urls = cfg.get('urls', [])
        if not urls:
            self._push_log('没有目标 URL')
            return

        # stats
        self.stats = {'sent':0,'success':0,'failed':0,'bytes':0,'start_time':time.time()}
        self._producer_done = False

        concurrency = max(1, int(cfg.get('concurrency', 50)))
        workers = max(1, int(cfg.get('workers', min(concurrency, 50))))
        queue_max = int(cfg.get('queue_max', max(1000, concurrency*10)))
        queue = asyncio.Queue(maxsize=queue_max)

        # rate limiter token bucket
        rate = int(cfg.get('rate',0))
        token_q = None
        refiller_task = None
        if rate > 0:
            token_q = asyncio.Queue(maxsize=max(1, rate*2))
            refiller_task = asyncio.create_task(self._refiller(rate, token_q))

        # connector & session (single session for whole run)
        connector = aiohttp.TCPConnector(limit=concurrency, ssl=cfg.get('ssl_verify', True))
        timeout = aiohttp.ClientTimeout(total=None)

        # create session
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            # create worker tasks
            worker_tasks = [asyncio.create_task(self._worker(f'W{i}', session, queue, token_q)) for i in range(workers)]
            # start producer
            producer_task = asyncio.create_task(self._producer(queue))

            # wait for producer to finish or stop
            while not self._stop.is_set():
                if producer_task.done():
                    break
                await asyncio.sleep(0.5)

            # If stop requested, cancel producer
            if not producer_task.done():
                producer_task.cancel()
                try:
                    await producer_task
                except Exception:
                    pass

            # 等待队列消费完或超时
            await queue.join()

            # 取消 workers
            for t in worker_tasks:
                t.cancel()
            # 等待 worker 任务结束
            try:
                await asyncio.gather(*worker_tasks, return_exceptions=True)
            except Exception:
                pass

            # 清理 refiller
            if refiller_task:
                refiller_task.cancel()
                try:
                    await refiller_task
                except Exception:
                    pass

        # session 自动关闭
        self._push_log('任务结束')
        self._push_stats()

# ---------------------- GUI ----------------------
class App:
    def __init__(self, root):
        self.root = root
        # ---------------- 免责声明 ----------------
        disclaimer = (
            "⚠️ 警告与免责声明 ⚠️\n\n"
            "1. 本程序仅用于你拥有或被授权的域名/资源。\n"
            "2. 滥用本程序对未授权网站或 CDN 触发流量可能违法，后果自负。\n"
            "3. 使用前请确保已获得合法授权。\n\n"
            "当前软件版本：v0.18\n\n"
            "点击 '确定' 同意继续使用，否则程序将退出。"
        )
        if not messagebox.askokcancel("免责声明", disclaimer):
            root.destroy()
            return
        # ---------------- 免责声明 END ----------------
        root.title('CDN 流量触发器 - 改进版')
        self.trigger = RobustCDNTrigger(ui_callback=self.ui_callback)

        frm = ttk.Frame(root, padding=8)
        frm.pack(fill=tk.BOTH, expand=True)

        left = ttk.Frame(frm)
        left.grid(row=0, column=0, sticky='nswe', padx=(0,8))
        right = ttk.Frame(frm)
        right.grid(row=0, column=1, sticky='nswe')

        frm.columnconfigure(0, weight=1)
        frm.columnconfigure(1, weight=0)
        frm.rowconfigure(0, weight=1)

        ttk.Label(left, text='目标 URL（每行一个）:').pack(anchor='w')
        self.txt_urls = scrolledtext.ScrolledText(left, height=8)
        self.txt_urls.pack(fill=tk.BOTH, expand=False)

        ttk.Label(left, text='代理列表（可选，每行一个, host:port 或 http...）:').pack(anchor='w', pady=(6,0))
        self.txt_proxies = scrolledtext.ScrolledText(left, height=4)
        self.txt_proxies.pack(fill=tk.BOTH, expand=False)

        params = ttk.Frame(left)
        params.pack(fill=tk.X, pady=(6,0))

        ttk.Label(params, text='并发(limit for connector):').grid(row=0, column=0, sticky='w')
        self.spin_concurrency = ttk.Spinbox(params, from_=1, to=2000, width=8)
        self.spin_concurrency.set('200')
        self.spin_concurrency.grid(row=0, column=1, sticky='w', padx=4)

        ttk.Label(params, text='Worker 数 (并发消费者):').grid(row=0, column=2, sticky='w', padx=(10,0))
        self.spin_workers = ttk.Spinbox(params, from_=1, to=1000, width=8)
        self.spin_workers.set('50')
        self.spin_workers.grid(row=0, column=3, sticky='w', padx=4)

        ttk.Label(params, text='每秒速率(req/s,0=不限制):').grid(row=1, column=0, sticky='w', pady=(6,0))
        self.spin_rate = ttk.Spinbox(params, from_=0, to=1000000, width=10)
        self.spin_rate.set('0')
        self.spin_rate.grid(row=1, column=1, sticky='w', padx=4)

        ttk.Label(params, text='请求超时(s):').grid(row=1, column=2, sticky='w', padx=(10,0))
        self.spin_timeout = ttk.Spinbox(params, from_=1, to=300, width=8)
        self.spin_timeout.set('15')
        self.spin_timeout.grid(row=1, column=3, sticky='w', padx=4)

        ttk.Label(params, text='持续时间(s,0=按总请求数控制):').grid(row=2, column=0, sticky='w', pady=(6,0))
        self.spin_duration = ttk.Spinbox(params, from_=0, to=86400, width=10)
        self.spin_duration.set('0')
        self.spin_duration.grid(row=2, column=1, sticky='w', padx=4)

        ttk.Label(params, text='总请求数(0=ignore):').grid(row=2, column=2, sticky='w', padx=(10,0))
        self.spin_total = ttk.Spinbox(params, from_=0, to=100000000, width=12)
        self.spin_total.set('10000')
        self.spin_total.grid(row=2, column=3, sticky='w', padx=4)

        ttk.Label(params, text='是否读取响应(read_response):').grid(row=3, column=0, sticky='w', pady=(6,0))
        self.var_read_resp = tk.BooleanVar(value=True)
        ttk.Checkbutton(params, variable=self.var_read_resp).grid(row=3, column=1, sticky='w', padx=4, pady=(6,0))

        ttk.Label(params, text='是否添加 cache-bust:').grid(row=3, column=2, sticky='w', padx=(10,0), pady=(6,0))
        self.var_cache_bust = tk.BooleanVar(value=True)
        ttk.Checkbutton(params, variable=self.var_cache_bust).grid(row=3, column=3, sticky='w', padx=4, pady=(6,0))

        ttk.Label(params, text='是否校验 SSL:').grid(row=4, column=0, sticky='w', pady=(6,0))
        self.var_ssl_verify = tk.BooleanVar(value=False)
        ttk.Checkbutton(params, variable=self.var_ssl_verify).grid(row=4, column=1, sticky='w', padx=4, pady=(6,0))

        ttk.Label(params, text='生产者 sleep(prod_sleep, s):').grid(row=4, column=2, sticky='w', padx=(10,0), pady=(6,0))
        self.spin_prod_sleep = ttk.Spinbox(params, from_=0, to=10, width=8)
        self.spin_prod_sleep.set('0')
        self.spin_prod_sleep.grid(row=4, column=3, sticky='w', padx=4, pady=(6,0))

        btns = ttk.Frame(left)
        btns.pack(fill=tk.X, pady=(8,0))
        self.btn_start = ttk.Button(btns, text='开始', command=self.on_start)
        self.btn_start.grid(row=0, column=0, padx=(0,6))
        self.btn_stop = ttk.Button(btns, text='停止', command=self.on_stop, state=tk.DISABLED)
        self.btn_stop.grid(row=0, column=1)
        self.btn_load_urls = ttk.Button(btns, text='从文件载入 URL', command=self.load_urls_from_file)
        self.btn_load_urls.grid(row=0, column=2, padx=(6,0))
        # ---------------------- GUI 底部信息 ----------------------
        link_frame = ttk.Frame(root)
        link_frame.pack(fill=tk.X, side=tk.BOTTOM, pady=(0, 4))

        def open_creator(event):
            webbrowser.open("https://space.bilibili.com/3546654714104666")

        def open_github(event):
            webbrowser.open("https://github.com/trrdrdrth/cdn")

        def open_toolbox(event):
            webbrowser.open("http://www.toolbox.ren")

        def open_torimg(event):
            webbrowser.open("http://www.torimg.com")

        lbl_creator = tk.Label(link_frame, text="创作者B站主页", fg="blue", cursor="hand2")
        lbl_creator.pack(side=tk.LEFT, padx=8)
        lbl_creator.bind("<Button-1>", open_creator)

        lbl_github = tk.Label(link_frame, text="GitHub 仓库", fg="blue", cursor="hand2")
        lbl_github.pack(side=tk.LEFT, padx=8)
        lbl_github.bind("<Button-1>", open_github)

        lbl_toolbox = tk.Label(link_frame, text="万能工具集", fg="blue", cursor="hand2")
        lbl_toolbox.pack(side=tk.LEFT, padx=8)
        lbl_toolbox.bind("<Button-1>", open_toolbox)

        lbl_torimg = tk.Label(link_frame, text="洋葱图床", fg="blue", cursor="hand2")
        lbl_torimg.pack(side=tk.LEFT, padx=8)
        lbl_torimg.bind("<Button-1>", open_torimg)

        ttk.Label(right, text='日志:').pack(anchor='w')
        self.txt_log = scrolledtext.ScrolledText(right, width=60, height=20, state=tk.NORMAL)
        self.txt_log.pack(fill=tk.BOTH, expand=True)

        stats_frame = ttk.Frame(right)
        stats_frame.pack(fill=tk.X, pady=(6,0))
        self.lbl_sent = ttk.Label(stats_frame, text='Sent: 0')
        self.lbl_sent.grid(row=0, column=0, sticky='w')
        self.lbl_success = ttk.Label(stats_frame, text='Success: 0')
        self.lbl_success.grid(row=0, column=1, sticky='w', padx=8)
        self.lbl_failed = ttk.Label(stats_frame, text='Failed: 0')
        self.lbl_failed.grid(row=0, column=2, sticky='w', padx=8)
        self.lbl_bytes = ttk.Label(stats_frame, text='Bytes: 0')
        self.lbl_bytes.grid(row=0, column=3, sticky='w', padx=8)

        self.status = ttk.Label(root, text='就绪', relief=tk.SUNKEN, anchor='w')
        self.status.pack(fill=tk.X, side=tk.BOTTOM)

    def load_urls_from_file(self):
        path = filedialog.askopenfilename(title='选择 URL 列表文件', filetypes=[('Text', '*.txt'), ('All', '*.*')])
        if not path:
            return
        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = f.read()
            self.txt_urls.delete('1.0', tk.END)
            self.txt_urls.insert(tk.END, data)
        except Exception as e:
            messagebox.showerror('错误', f'载入失败: {e}')

    def on_start(self):
        urls = parse_urls(self.txt_urls.get('1.0', tk.END))
        if not urls:
            messagebox.showwarning('警告', '请填写至少一个目标 URL')
            return
        proxies = parse_proxies(self.txt_proxies.get('1.0', tk.END))
        try:
            concurrency = int(self.spin_concurrency.get())
            workers = int(self.spin_workers.get())
            rate = int(self.spin_rate.get())
            timeout = int(self.spin_timeout.get())
            duration = int(self.spin_duration.get())
            total = int(self.spin_total.get())
            prod_sleep = float(self.spin_prod_sleep.get())
        except Exception as e:
            messagebox.showerror('错误', f'参数解析失败: {e}')
            return

        config = {
            'urls': urls,
            'proxies': proxies,
            'concurrency': concurrency,
            'workers': workers,
            'rate': rate,
            'req_timeout': timeout,
            'duration': duration,
            'total': total,
            'read_response': self.var_read_resp.get(),
            'cache_bust': self.var_cache_bust.get(),
            'ssl_verify': self.var_ssl_verify.get(),
            'user_agents': [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15',
                'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1',
                'Mozilla/5.0 (iPad; CPU OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1',
                'Mozilla/5.0 (Android 13; Mobile; rv:120.0) Gecko/120.0 Firefox/120.0',
                'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)'
            ],

            'prod_sleep': prod_sleep,
            'queue_max': max(1000, concurrency*10)
        }

        # UI
        self.btn_start['state'] = tk.DISABLED
        self.btn_stop['state'] = tk.NORMAL
        self.status['text'] = '运行中...'
        self.txt_log.delete('1.0', tk.END)

        try:
            self.trigger.start(config)
            self._updater = threading.Thread(target=self._ui_updater_loop, daemon=True)
            self._updater.start()
        except Exception as e:
            messagebox.showerror('错误', f'启动失败: {e}')
            self.btn_start['state'] = tk.NORMAL
            self.btn_stop['state'] = tk.DISABLED
            self.status['text'] = '就绪'

    def on_stop(self):
        self.trigger.stop()
        self.status['text'] = '停止中...'
        self.btn_stop['state'] = tk.DISABLED

    def ui_callback(self, typ, payload):
        if typ == 'log':
            self._append_log(payload)
        elif typ == 'stat':
            self._update_stats(payload)

    def _append_log(self, text):
        def _do():
            self.txt_log.insert(tk.END, f"[{time.strftime('%H:%M:%S')}] {text}\n")
            self.txt_log.see(tk.END)

            self.txt_log.see(tk.END)
        self.root.after(0, _do)

    def _update_stats(self, stats):
        def _do():
            self.lbl_sent['text'] = f"Sent: {stats.get('sent',0)}"
            self.lbl_success['text'] = f"Success: {stats.get('success',0)}"
            self.lbl_failed['text'] = f"Failed: {stats.get('failed',0)}"
            self.lbl_bytes['text'] = f"Bytes: {stats.get('bytes',0)}"
            elapsed = int(time.time() - stats.get('start_time', time.time()))
            self.status['text'] = f"运行中 - 已发送 {stats.get('sent',0)} 请求 - 用时 {elapsed}s"
        self.root.after(0, _do)

    def _ui_updater_loop(self):
        while True:
            if not (self.trigger._thread and self.trigger._thread.is_alive()):
                break
            try:
                self.ui_callback('stat', dict(self.trigger.stats))
            except Exception:
                pass
            time.sleep(0.5)
        def _done():
            self.btn_start['state'] = tk.NORMAL
            self.btn_stop['state'] = tk.DISABLED
            self.status['text'] = '就绪'
        self.root.after(0, _done)

if __name__ == '__main__':
    root = tk.Tk()
    app = App(root)
    root.geometry('1100x650')
    root.mainloop()
