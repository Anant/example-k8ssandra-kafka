const Kafka = require('node-rdkafka')
const fetch = require("node-fetch")


function addData(Title,Description,Channel){
    return fetch(`http://localhost:8082/v2/keyspaces/youtube/Results`,{
        method:`POST`,
        headers: {
            'content-type': 'application/json',
            'X-Cassandra-Token' : 'f91350b0-1d5a-4791-82ff-c3330b6b7b78'
        },
        body: JSON.stringify({
            Title,
            Description,
            Channel
        })
    })
}

const consumer = Kafka.KafkaConsumer({
    'group.id': 'kafka',
    'metadata.broker.list': 'localhost:9092',
}, {});

consumer.connect();

consumer.on('ready',() =>{
    console.log('Consumer ready')
    consumer.subscribe(['k8ssandra'])
    consumer.consume()
}).on('data', (data) => {
    let stream = data.value
    let res = JSON.parse(stream.toString('utf8'))
    addData(res.id,res.Title,res.Description,res.Channel)

    console.log(res)
})