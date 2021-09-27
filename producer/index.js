const Kafka = require('node-rdkafka')
const avro = require('avsc')
const fetch = require("node-fetch")


const stream = Kafka.Producer.createWriteStream({
    'metadata.broker.list': 'localhost:9092'
}, {}, {
    topic: 'k8ssandra'
});

function apiCall() {
    const api = `https://youtube.googleapis.com/youtube/v3/search?part=snippet&q=kubernetes&key=AIzaSyDtLUZS1tgAGfV-u-_03JUl5zvja-7GqQI&maxResults=30&order=viewCount`
        fetch(`${api}`).then(
            res => {
                return res.json()
            }
        ).then(
            json => {
                for (let i = 0; i < 50; i++) {
                    let fields = json.items[i]
                    let message = {
                        "Title": fields.snippet.title,
                        "Description": fields.snippet.description,
                        "Channel": fields.snippet.channelTitle,
                        "id": fields.id.videoId
                    }
                    const result = stream.write(JSON.stringify(message))
                    if (result) {
                        console.log(`Sucsess ${i + 1}`)
                    }
                    else {
                        console.log('Fail')
                    }
                }

            }
        )  
}

apiCall()
