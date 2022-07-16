import client, { Connection, Channel, ConsumeMessage } from 'amqplib'
import { Command } from 'commander';

const program = new Command();

program
    .name('queue-republisher')
    .description('simmple command line script to republish messages from one queue to another')
    .version('0.0.1');

program
    .option('-c, --connection-string <connection-string>', 'connection string to rabbitmq server', 'amqp://localhost:5672')
    .option('-s, --source-queue <source-queue>', 'source queue name', 'source-queue')
    .option('-t, --target-queue <target-queue>', 'target queue name', 'target-queue');

program.parse(process.argv);
const options = program.opts();
const connectionString: string = options.connectionString;
const sourceQueueName: string = options.sourceQueue;
const targetQueueName: string = options.targetQueue;
(async () => {
    const messageHandler = (content: string): void => {
        targetChannel.sendToQueue(targetQueueName, Buffer.from(content));
    } 
    const consumer = (channel: Channel) => (msg: ConsumeMessage | null): void => {
        if(msg) {
            const messageContent: string = msg.content.toString();
            console.log(messageContent);
            messageHandler(messageContent);
            channel.ack(msg);
        } else {
            throw new Error('Error');
        }
    }
    const connection: Connection = await client.connect(connectionString);

    const targetChannel: Channel = await connection.createChannel();
    await targetChannel.assertQueue(targetQueueName);

    const sourceChannel: Channel = await connection.createChannel();
    await sourceChannel.assertQueue(sourceQueueName);
    await sourceChannel.consume(sourceQueueName, consumer(sourceChannel));
})();

