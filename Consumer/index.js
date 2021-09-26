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

function addData(id,Title,Description,Channel){
    return fetch(`http://localhost:9200/newnewindex/data/${id}`,{
        method:`POST`,
        headers: {
            'content-type': 'application/json'
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
    consumer.subscribe(['elassandra'])
    consumer.consume()
}).on('data', (data) => {
    let stream = data.value
    let res = JSON.parse(stream.toString('utf8'))
    addData(res.id,res.Title,res.Description,res.Channel)

    console.log(res)
})