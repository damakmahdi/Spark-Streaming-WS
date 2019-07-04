# Spark-Streaming-WS
The purpose of this Project is to send Spark streaming data collected from an MQTT broker directly to browser in order to visualize them.
First step is to prepare the streaming using Spark.
Second step is to create a WebSocket client, which will recieve data from Spark, and send them to an EndPoint Websocket Server.
In the other hand, a HTML client is permanently subscribed to the Websocket server, which will give him the access to get all data coming from spark Streaming.
On every message recieved, we add the data into a chart.js Chart.
