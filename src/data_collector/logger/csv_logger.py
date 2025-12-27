import multiprocessing as mp
import os
import csv
import time
import copy

class csv_logger:
    """Multiprocess-safe CSV writer for tick data."""
    def __init__(self, base_path="E:/DB", num_workers=None,FLUSH_SIZE = 20,queue_maxsize=20000, put_timeout=1.0):
        ctx = mp.get_context("spawn")
        self.base_path = base_path
        self.queue = ctx.Queue(maxsize=queue_maxsize)
        self.put_timeout = put_timeout
        self.flush_size = FLUSH_SIZE

        num_workers = num_workers if num_workers is not None else max(2, (os.cpu_count() or 2) // 2)
        
        self.workers = []
        for i in range(num_workers):
            p = ctx.Process(target=csv_writer_worker, args=(self.queue, self.flush_size), name=f"writer-{i}")
            p.daemon = False
            p.start()
            self.workers.append(p)
            

    # --- Path Sanitation ---
    def _sanitize_path(self, path):
        """Ensure the file path is always inside BASE_PATH."""
        path = path.replace("\\", "/")
        if ":" in path.split("/")[0]:
            path = "/".join(path.split("/")[1:])
        return os.path.join(self.base_path, path.lstrip("/"))

    # --- Async Save ---
    def save_async(self, path, data, cols, is_market_depth=False, retry=3):
        """Queue data for writing asynchronously (safe, no corruption)."""
        try:
            # ----- Handle market depth folder rename -----
            if is_market_depth:
                parts = path.split("/")
                if len(parts) >= 2:
                    folder = parts[-2]
                    mob_folder = folder + "-MOB"
                    path = path.replace(f"/{folder}/", f"/{mob_folder}/", 1)

            full_path = self._sanitize_path(path)

            safe_data = copy.deepcopy(data)
            safe_cols = tuple(cols)

            # ----- Try to enqueue -----
            for attempt in range(retry):
                try:
                    self.queue.put((full_path, safe_data, safe_cols), timeout=self.put_timeout)
                    return True
                except mp.queues.Full:
                    time.sleep(0.1 * (attempt + 1))

            # ----- Final fallback: synchronous write -----
            flush_buffer(full_path, safe_data, safe_cols)
            print(f"[Logger] Fallback sync write for {os.path.basename(full_path)}")
            return True

        except Exception as e:
            print(f"[Logger] Failed save_async: {e}")
            return False

    # --- Graceful Shutdown ---
    def flush_all(self, join_timeout=30):
        """Flush all data and stop writer processes cleanly."""
        print("[Logger] Flushing all queues...")
        if not self.workers:
            return
        
        for _ in self.workers:
            self.queue.put(None)

        start = time.time()
        for p in self.workers:
            p.join(timeout=max(0.1, join_timeout - (time.time() - start)))
            if p.is_alive():
                print(f"[Logger] {p.name} did not exit in time. Terminating...")
                p.terminate()
        print("✅ [Logger] All writer processes exited cleanly.")


def csv_writer_worker(queue,flush_size):
        """Dedicated worker process for writing CSV data safely."""
        buffers = {}  # path -> (rows, cols)
        try:
            while True:
                msg = queue.get()
                if msg is None:
                    # Flush remaining before exit
                    for path, (rows, cols) in buffers.items():
                        if rows:
                            flush_buffer(path, rows, cols)
                    print(f"[{mp.current_process().name}] Exited safely.")
                    break

                path, data, cols = msg
                if path not in buffers:
                    buffers[path] = ([], cols)
                rows, header = buffers[path]
                rows.extend(data)

                if len(rows) >= flush_size:
                    flush_buffer(path, rows, header)
                    buffers[path] = ([], header)
        except KeyboardInterrupt:
            for path, (rows, cols) in buffers.items():
                if rows:
                    flush_buffer(path, rows, cols)
        except Exception as e:
            print(f"[Writer] Unexpected error: {e}")

def flush_buffer(path, rows, cols):
        """Perform the actual CSV write (atomic per call)."""
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            file_exists = os.path.exists(path)
            write_header = not file_exists or os.path.getsize(path) == 0

            with open(path, "a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                if write_header:
                    writer.writerow(cols)
                writer.writerows(rows)
        except Exception as e:
            print(f"[Writer] Flush error ({path}): {e}")