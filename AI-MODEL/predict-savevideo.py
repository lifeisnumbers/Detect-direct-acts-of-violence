import cv2
from PIL import Image
from skimage.transform import resize
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
from model import *


in_topic_name = "Camera_in"
out_topic_name = "prediction_value"
out_video_topic_name = "save_video"
kafka_ip='localhost:9092' #insert ip 
consumer = KafkaConsumer(
    in_topic_name,
    bootstrap_servers=kafka_ip,
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id=None,
    fetch_max_bytes=52428800,
    fetch_max_wait_ms = 10000
)

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_ip,
    max_request_size=9048576,
    compression_type = 'gzip'
)


model = mamon_videoFightModel2(tf)

print('running')

frames = np.zeros((30, 160, 160, 3), dtype=float)
nodatav = np.zeros((1, 30, 160, 160, 3), dtype=float)
i = 0 
for message in consumer:
    stream = message.value
    #chuyển thành hình ảnh
    image = cv2.imdecode(np.frombuffer(stream, dtype=np.uint8), cv2.IMREAD_COLOR)

    # cv2.putText(image, "PROCESSED", (100,100), cv2.FONT_HERSHEY_SIMPLEX, 2, (255,255,255),2)
    # print("Send to kafka")
    ret, frame = cv2.imencode('.jpg', image)
    # print(frame)
    # print(frame)

    
    if ret:  
        frm = resize(image,(160,160,3))
        frm = np.expand_dims(frm,axis=0)
        if(np.max(frm)>1):
            frm = frm/255.0
                
        frames[i][:] = frm
        nodatav[0][:][:] = frames
        i+=1
        # print(i)
        if i == 30:
            kq, percent = pred_fight(model, nodatav, acuracy=0.9)
            i=0
            print(kq)
            # if percent > 0.8:
            #     jpg_data = frame.tobytes()
            #     with open('output_frame.jpg', 'wb') as file:
            #         file.write(jpg_data)
            print("percent")
            print(percent)
        
                
                
        # print(kq)
        # print(frames)
        # if kq: 
        #         # producer.send(out_video_topic_name, frames.tobytes())
        #         producer.send( out_topic_name , kq)#.encode("utf-8"))
        # producer.send(out_video_topic_name, frame.tobytes)
        # producer.flush()
        
   

