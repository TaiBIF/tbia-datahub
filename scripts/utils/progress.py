import atexit
import time
import sys
from collections import defaultdict
from contextlib import contextmanager
from functools import wraps

class StepTimer:
    def __init__(self):
        self.timings = defaultdict(list)
        self._marker = defaultdict(int)
    
    @contextmanager
    def __call__(self, name):
        start = time.perf_counter()
        try:
            yield
        finally:
            self.timings[name].append(time.perf_counter() - start)
    
    def batch_summary(self, label=""):
        rows = []
        for name, ts in self.timings.items():
            since = ts[self._marker[name]:]
            if since:
                rows.append((name, sum(since), len(since)))
        if not rows:
            return
        rows.sort(key=lambda x: -x[1])
        total = sum(r[1] for r in rows)
        try:
            from tqdm import tqdm
            write = tqdm.write
        except ImportError:
            write = print
        write(f"⏱  {label} 本批 {total:.1f}s")
        for name, t, n in rows:
            write(f"     {name:<28} {t:>6.2f}s × {n}")
        for name in self.timings:
            self._marker[name] = len(self.timings[name])
    
    def summary(self):
        if not self.timings:
            return
        print("\n=== 累計耗時 ===")
        for name, ts in sorted(self.timings.items(), key=lambda x: -sum(x[1])):
            total = sum(ts)
            print(f"  {name:<28} {total:>8.2f}s  (平均 {total/len(ts):>5.2f}s × {len(ts)})")

timer = StepTimer()

# 只有正常跑完（沒有未捕捉的例外）才印累計耗時；中途 raise error 中斷時不印
_interrupted = False
_orig_excepthook = sys.excepthook

def _excepthook(exc_type, exc_value, exc_tb):
    global _interrupted
    _interrupted = True
    _orig_excepthook(exc_type, exc_value, exc_tb)

sys.excepthook = _excepthook

@atexit.register
def _final_summary():
    if not _interrupted:
        timer.summary()

def timed(name=None):
    def deco(fn):
        step_name = name or fn.__name__
        @wraps(fn)
        def wrapper(*args, **kwargs):
            with timer(step_name):
                return fn(*args, **kwargs)
        return wrapper   # ← 補上這行
    return deco