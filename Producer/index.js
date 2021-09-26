const Kafka = require('node-rdkafka')
const avro = require('avsc')
const fetch = require("node-fetch")


const type = avro.Type.forSchema({
    type: 'record',
    fields: [
        {
            name: 'category',
            type: {
                type: 'enum',
                symbols: ['CAT', 'DOG']
            }
        },
        {
            name: 'name',
            type: 'string'
        }
    ]
});

const stream = Kafka.Producer.createWriteStream({
    'metadata.broker.list': 'localhost:9092'
}, {}, {
    topic: 'elassandra'
});

function apiCall() {
    const api = `https://youtube.googleapis.com/youtube/v3/search?part=snippet&q=java&key=AIzaSyDtLUZS1tgAGfV-u-_03JUl5zvja-7GqQI&maxResults=50&order=viewCount`
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