import threading
from queue import Queue, Empty
import signal
import time


class CoffeeShop:
    def __init__(self):
        self.orders_queue = Queue()
        self.ready_orders = Queue()
        self.should_stop = threading.Event()

    def take_order(self, order_id):
        if not self.should_stop.is_set():
            print(f"收到订单#{order_id}")
            self.orders_queue.put(order_id)

    def make_coffee(self):
        while not self.should_stop.is_set():
            try:
                order_id = self.orders_queue.get(timeout=1)
                print(f"正在制作订单#{order_id}的咖啡...")
                time.sleep(2)  # 模拟制作咖啡的时间
                self.ready_orders.put(order_id)
                print(f"订单#{order_id}的咖啡制作完成")
                self.orders_queue.task_done()
            except Empty:
                continue

    def serve_coffee(self):
        while not self.should_stop.is_set():
            try:
                order_id = self.ready_orders.get(timeout=1)
                print(f"服务员正在派送订单#{order_id}")
                time.sleep(1)  # 模拟派送时间
                print(f"订单#{order_id}已送达!")
                self.ready_orders.task_done()
            except Empty:
                continue


def run_coffee_shop():
    shop = CoffeeShop()

    # 创建工作线程
    threads = [
        threading.Thread(target=shop.make_coffee, name="Barista"),
        threading.Thread(target=shop.serve_coffee, name="Server"),
    ]

    # 将线程设置为守护线程
    for thread in threads:
        thread.daemon = True
        thread.start()

    def handle_shutdown(signum, frame):
        print("\n正在关闭咖啡店...")
        shop.should_stop.set()

    # 设置信号处理
    signal.signal(signal.SIGINT, handle_shutdown)

    try:
        # 模拟接收订单
        for i in range(5):
            shop.take_order(i)
            time.sleep(0.5)

        # 等待所有订单处理完成
        shop.orders_queue.join()
        shop.ready_orders.join()

    except KeyboardInterrupt:
        print("\n收到中断信号，正在关闭...")
    finally:
        # 设置停止标志
        shop.should_stop.set()

        # 等待所有线程完成
        for thread in threads:
            thread.join(timeout=2)

        print("咖啡店已关闭")


if __name__ == "__main__":
    run_coffee_shop()
