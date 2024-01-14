import cv2
from PIL import Image
import config
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
from model import *


in_topic_name = "camera_in"
# out_topic_name = "video_out"
kafka_ip='' #insert ip 
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
    bootstrap_servers=[config.ip],
    max_request_size=9048576,
    compression_type = 'gzip'
)
model = mamon_videoFightModel2(tf, wight='AI-MODEL/mamonbest947oscombo-drive.hdfs')


frames = np.zeros((30, 160, 160, 3), dtype=np.float)
i = 0 
for message in consumer:
    stream = message.value
    image = cv2.imdecode(np.frombuffer(stream, dtype=np.uint8), cv2.IMREAD_COLOR)
    print(image.shape)

    cv2.putText(image, "PROCESSED", (100,100), cv2.FONT_HERSHEY_SIMPLEX, 2, (255,255,255),2)
    print("Send to kafka")
    ret, frame = cv2.imencode('.jpg', image)
    while i < 30:
        frm = resize(frame,(160,160,3))
        frm = np.expand_dims(frm,axis=0)
        if(np.max(frm)>1):
            frm = frm/255.0
        frames[i][:] = frm
        i +=1
    kq, percent = pred_fight(model, frames, acuracy=0.6)
    print(kq)
    print(frames)

  
    # producer.send( out_topic_name , buffer.tobytes())#.encode("utf-8"))
    producer.flush()
