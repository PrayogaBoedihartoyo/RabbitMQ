const amqp = require('amqplib');

const QUEUE_NAME = 'user_queue';

async function consumeMessages() {
    try {
        // Membuat koneksi ke server RabbitMQ
        const connection = await amqp.connect('amqp://127.0.0.1');
        const channel = await connection.createChannel();

        // Membuat atau memastikan antrian (queue) sudah ada
        await channel.assertQueue(QUEUE_NAME, { durable: true });

        // Mengkonsumsi pesan dari antrian (queue)
        await channel.consume(QUEUE_NAME, (msg) => {
            const message = msg.content.toString();
            console.log('Received message:', message);

            // Lakukan sesuatu dengan pesan yang diterima

            // Konfirmasi pesan telah diproses (acknowledge)
            channel.ack(msg);
        });
    } catch (error) {
        console.error('Error consuming messages:', error);
    }
}

consumeMessages().then(r => console.log('Consumer started'));
