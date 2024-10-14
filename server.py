import asyncio
import os

from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, UploadFile, File
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

# Cấu hình
redpanda_server = "localhost:9092"
request_topic = "image-input"
response_topic = "image-output"

# Viết class subscribe vào redpanda
@asynccontextmanager
async def startup(app):
    app.producer = AIOKafkaProducer(bootstrap_servers=[redpanda_server])
    await app.producer.start()

    app.consumer =AIOKafkaConsumer(
        response_topic,
        group_id="image-reply-group"
    )

    await app.consumer.start()

    yield

app = FastAPI(lifespan=startup)

# Định nghĩa thư mục static
app.mount("/static", StaticFiles(directory="static"), name = "static")

# Định nghĩa các điểm endpoint
@app.get("/")
async def read_index():
    return  FileResponse("static/index.html")


@app.post("/upload-image")
async def upload_image(file: UploadFile = File(...)):

    # Save file
    current_dir = os.path.dirname(os.path.realpath(__file__))
    file_location  = os.path.join(current_dir, "static/images/" + file.filename)

    print("Save file to ", file_location)
    with open(file_location, "wb") as f:
        f.write(await file.read())

    await app.producer.send(request_topic, file.filename.encode('utf-8'))

@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await websocket.accept()

    async def return_message(msg):
        await websocket.send_text(msg.value.decode('utf-8'))

    async def consume_from_topic(topic, callback):
        print("Receive from topic ", topic)
        async for msg in app.consumer:
            print("Receive :", msg.value.decode('utf-8'))
            await callback(msg)

    asyncio.create_task(consume_from_topic(response_topic, return_message))
    while True:
        await asyncio.sleep(10)