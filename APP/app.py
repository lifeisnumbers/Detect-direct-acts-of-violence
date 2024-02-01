from flask import Flask, render_template, Response, send_file,jsonify,url_for,send_from_directory
import cv2
import numpy as np
import imageio
from kafka import KafkaConsumer
from io import BytesIO
import os
from model import *
from skimage.transform import resize
import time 
app = Flask(__name__)

in_topic_name = "video501"
kafka_ip ='localhost:9092'  # Thay đổi địa chỉ IP của Kafka nếu cần
consumer = KafkaConsumer(
    in_topic_name,
    bootstrap_servers=kafka_ip,
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id=None,
    fetch_max_bytes=52428800,
    fetch_max_wait_ms=10000
)

model = mamon_videoFightModel2(tf)

class VideoProcessor:
    def __init__(self):
        self.frames = np.zeros((30, 160, 160, 3), dtype=float)
        self.nodatav = np.zeros((1, 30, 160, 160, 3), dtype=float)
        self.i = 0
        self.alert_frame = None  # Để lưu frame khi có cảnh báo
        self.current_percent = 0.0  # Để lưu giá trị percent hiện tại
    def get_current_percent(self):
        return self.current_percent    
    def process_frame(self, image):
        ret, frame = cv2.imencode('.jpg', image)

        if ret:
            frm = resize(image, (160, 160, 3))
            frm = np.expand_dims(frm, axis=0)
            if np.max(frm) > 1:
                frm = frm / 255.0

            self.frames[self.i][:] = frm
            self.nodatav[0][:][:] = self.frames
            self.i += 1

            if self.i == 30:
                kq, percent = pred_fight(model, self.nodatav, acuracy=0.9)
                print(kq)
                print("percent")
                print(percent)
                self.current_percent = percent 
                if percent >= 0.9:
                    print("Cảnh báo có đánh nhau!")
                    print("Percent: ", percent)
                    print("Percent Format: {:.2%}".format(percent))
                    # generate_alert_frame()
                    self.alert_frame = image.copy()  # Lưu frame khi có cảnh báo
                    # Thực hiện xử lý khi có đánh nhau, ví dụ: lưu frame, gửi thông báo, vv.
                self.i = 0

            _, buffer = cv2.imencode('.jpg', image)
            frame = buffer.tobytes()
            return frame

    def get_alert_frame(self):
        return self.alert_frame
    

video_processor = VideoProcessor()

@app.route('/')
def index():
    percent = video_processor.get_current_percent()
    return render_template('index3.html', percent=percent)
@app.route('/percent')
def get_percent():
    percent = video_processor.get_current_percent()
    percent_float = float(percent) 
    return jsonify(percent=percent_float)
def generate_frames():
    for message in consumer:
        stream = message.value
        image = cv2.imdecode(np.frombuffer(stream, dtype=np.uint8), cv2.IMREAD_COLOR)
        frame = video_processor.process_frame(image)
        yield (b'--frame\r\n'
               b'Content-Type: image/jpeg\r\n\r\n' + frame + b'\r\n') # Biến để theo dõi thời gian cảnh báo cuối cùng
def generate_alert_frame():
    global last_alert_time
    global alert_frame_counter
    frame = video_processor.get_alert_frame()
    print("alert_frame_counter:", alert_frame_counter)
    if frame is not None:
        _, buffer = cv2.imencode('.jpg', frame)

        alert_folder = "static\\output"

        if not os.path.exists(alert_folder):
            os.makedirs(alert_folder)
        
        alert_filename = os.path.join(alert_folder, f'alert_frame_{alert_frame_counter:04d}.jpg')

        with open(alert_filename, 'wb') as file:
            file.write(buffer.tobytes())

        last_alert_time = time.time()
        alert_frame_counter += 1
        return alert_filename
    else:
        return None

alert_frame_counter = 0  # Khởi tạo biến đếm 
@app.route('/generate_alert_frame')
def generate_alert_frame_route():
    # Call your generate_alert_frame() function here
    alert_frame_path = generate_alert_frame()

    # Return a response, you can customize this based on your needs
    return jsonify({"alert_frame_path": alert_frame_path})
@app.route('/video_feed') 
def video_feed():
    return Response(generate_frames(), mimetype='multipart/x-mixed-replace; boundary=frame')
if __name__ == '__main__':
    app.run(debug=True)