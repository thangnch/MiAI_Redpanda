import asyncio
import os
import time

from PIL import Image, ImageOps
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

import cv2
from ultralytics import YOLO


# Cấu hình
redpanda_server = "localhost:9092"
request_topic = "image-input"
response_topic = "image-output"

current_dir = os.path.dirname(os.path.realpath(__file__))
images_path = os.path.join(current_dir, "static/images")

model = YOLO("yolov8n.pt") # khoi tao Yolov8. LLM, VisionTransformer

def predict(chosen_model, img, classes=[], conf=0.5):
    if classes:
        results = chosen_model.predict(img, classes=classes, conf=conf)
    else:
        results = chosen_model.predict(img, conf=conf)

    return results

def predict_and_detect(chosen_model, img, classes=[], conf=0.5, rectangle_thickness=2, text_thickness=1):
    results = predict(chosen_model, img, classes, conf=conf)
    for result in results:
        for box in result.boxes:
            cv2.rectangle(img, (int(box.xyxy[0][0]), int(box.xyxy[0][1])),
                          (int(box.xyxy[0][2]), int(box.xyxy[0][3])), (255, 0, 0), rectangle_thickness)
            cv2.putText(img, f"{result.names[int(box.cls[0])]}",
                        (int(box.xyxy[0][0]), int(box.xyxy[0][1]) - 10),
                        cv2.FONT_HERSHEY_PLAIN, 1, (255, 0, 0), text_thickness)
    return img, results


# Hàm đọc từ IN-TOPIC (request_topic)
async def read_requests():

    consumer = AIOKafkaConsumer(
        request_topic,
        bootstrap_servers=redpanda_server,
        group_id= "image-process-group"
    )

    await consumer.start()
    try:
        async for msg in consumer:
            filename = msg.value.decode('utf-8')
            await process_image(filename)
    finally:
        await consumer.stop()

async def send_back_to_queue(topic, message):
    producer = AIOKafkaProducer(bootstrap_servers=redpanda_server)
    await producer.start()

    try:
        await producer.send_and_wait(topic, message.encode("utf-8"))
    finally:
        await producer.stop()

async def process_image(filename):
    # Xử lý bất cứ cái gì bạn muốn: LLM, Vision
    try:
        image = cv2.imread(os.path.join(images_path, filename))
        result_image, _ = predict_and_detect(model, image, classes=[], conf=0.5)
        new_file = "yolo_" + filename
        cv2.imwrite(os.path.join(images_path, new_file), result_image)
        # Trả về OUT TOPIC - response_topic
        await send_back_to_queue(response_topic, new_file)

    except Exception as ex:
        print(ex)


print("Start image service")
asyncio.run(read_requests())
