# run.py
from app import app
import webbrowser
from threading import Timer
from flask import request # MỚI: Thêm thư viện để xử lý request
import os              # MỚI: Thêm thư viện để tương tác với hệ điều hành
import signal          # MỚI: Thêm thư viện để gửi tín hiệu

# MỚI: Thêm route để xử lý việc tắt server
@app.route('/shutdown', methods=['POST'])
def shutdown():
    """Tắt máy chủ một cách an toàn."""
    print("Yêu cầu tắt máy chủ đã được nhận...")
    os.kill(os.getpid(), signal.SIGTERM)
    return 'Máy chủ đang tắt...'

def open_browser():
    """Hàm để mở trình duyệt sau một khoảng trễ ngắn."""
    webbrowser.open_new("http://127.0.0.1:5000")

if __name__ == '__main__':
    # Hẹn giờ để mở trình duyệt sau 1 giây, đảm bảo server đã khởi động
    Timer(1, open_browser).start()
    # Chạy ứng dụng
    app.run(host='127.0.0.1', port=5000, debug=False)