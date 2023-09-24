const express = require('express');
const { Client } = require('pg');
const amqp = require('amqplib');
const bodyParser = require("body-parser");
const app = express();
const port = 3000;
app.use(bodyParser.json());

// Konfigurasi PostgreSQL
const pgConfig = {
    host: 'localhost',
    port: 5432,
    database: 'postgres',
    user: 'postgres',
    password: 'postgres',
};

// Konfigurasi RabbitMQ
const rabbitMQConfig = {
    host: '127.0.0.1',
    port: 5672,
    username: 'guest',
    password: 'guest',
};

// Membuat koneksi ke PostgreSQL
const pgClient = new Client(pgConfig);
pgClient.connect();

// Membuat koneksi ke RabbitMQ
let rabbitMQConnection;
let rabbitMQChannel;

async function connectToRabbitMQ() {
    rabbitMQConnection = await amqp.connect(`amqp://${rabbitMQConfig.host}:${rabbitMQConfig.port}`, {
        username: rabbitMQConfig.username,
        password: rabbitMQConfig.password,
    });

    rabbitMQChannel = await rabbitMQConnection.createChannel();

    // Membuat exchange dan queue
    const exchange = 'user_exchange';
    const queue = 'user_queue';
    const routingKey = 'user.create';

    await rabbitMQChannel.assertExchange(exchange, 'direct', { durable: true });
    await rabbitMQChannel.assertQueue(queue, { durable: true });
    await rabbitMQChannel.bindQueue(queue, exchange, routingKey);
}

// Mengambil data pengguna dari PostgreSQL
async function getUserData() {
    const query = 'SELECT * FROM users';
    const result = await pgClient.query(query);
    return result.rows;
}

// Mengirim pesan ke RabbitMQ
async function sendMessageToRabbitMQ(message) {
    const exchange = 'user_exchange';
    const routingKey = 'user.create';

    await rabbitMQChannel.publish(exchange, routingKey, Buffer.from(JSON.stringify(message)), { persistent: true });
}

// Endpoint untuk mengambil data pengguna
app.get('/users', async (req, res) => {
    try {
        const userData = await getUserData();
        res.status(200).json(userData);
    } catch (error) {
        console.error('Error:', error);
        res.status(500).json({ message: 'Internal Server Error' });
    }
});

// Endpoint untuk membuat pengguna baru dan mengirim pesan ke RabbitMQ
app.post('/users', async (req, res) => {
    try {
        // Simulasi pembuatan pengguna baru
        const newUser = {
            name: req.body.name,
        };

        await pgClient.query('INSERT INTO users (name) VALUES ($1)', [newUser.name]);

        // Kirim pesan ke RabbitMQ
        await sendMessageToRabbitMQ(newUser);

        res.status(200).json({ message: 'User created and message sent to RabbitMQ' });
    } catch (error) {
        console.error('Error:', error);
        res.status(500).json({ message: 'Internal Server Error' });
    }
});

// Menjalankan server dan koneksi ke RabbitMQ
app.listen(port, async () => {
    console.log(`Server running on port http://localhost:${port}`);

    // Membuat koneksi ke RabbitMQ
    try {
        await connectToRabbitMQ();
        console.log('Connected to RabbitMQ');
    } catch (error) {
        console.error('Error connecting to RabbitMQ:', error);
    }
});
