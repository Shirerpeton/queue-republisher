import client, { Connection, Channel, ConsumeMessage } from 'amqplib'
import { Command } from 'commander';

const program = new Command();

program
    .name('queue-republisher')
    .description('simmple command line script to republish messages from one queue to another')
    .version('1.0.0');

program
    .option('-c, --connection-string <connection-string>', 'connection string to rabbitmq server', 'amqp://localhost:5672')
    .option('-s, --source-queue <source-queue>', 'source queue name', 'source-queue')
    .option('-t, --target-queue <target-queue>', 'target queue name', 'target-queue')
    .option('-m, --message-count <message-count>', 'number of messages to republish (set 0 or leave blank to not set a limit)', '0')
    .option('-d, --delay <delay>', 'delay in milliseconds between each message', '0')
    .option('-f, --filter <filter>', 'regex filter to apply message content (all messages that conform to filter will be repubslished to target queue and all otther will be republished back into source queue)', '')

program.parse(process.argv);
const options = program.opts();
const connectionString: string = options.connectionString;
const sourceQueueName: string = options.sourceQueue;
const targetQueueName: string = options.targetQueue;
let messageCount: number = parseInt(options.messageCount);
const delay: number = parseInt(options.delay);
const filter: string = options.filter;

const closeChannel = async (channel: Channel): Promise<void> => {
    if (channel) await channel.close();
}

const closeConnection = async (connection: Connection): Promise<void> => {
    if (connection) await connection.close();
}

(async (): Promise<void> => {
    const getMessageHandler = (channel: Channel, queueName: string): ((content: string) => void) => {
        return content => channel.sendToQueue(queueName, Buffer.from(content));
    };
    const consumeMessage = async (targetHandler: (content: string) => void, sourceHandler: (content: string) => void, delay: number): Promise<boolean> => {
        const message: false | client.GetMessage = await sourceChannel.get(sourceQueueName);
        if(message == false) {
            console.log('No messages in queue');
            return false;
        }
        const messageContent: string = message.content.toString();
        console.log('Message received: ' + messageContent);
        if(filter == '' || messageContent.match(filter)) {
            console.log('publishing to target queue');
            targetHandler(messageContent);
        } else {
            console.log('publishing to source queue');
            sourceHandler(messageContent);
        }
        sourceChannel.ack(message);
        return new Promise(resolve => setTimeout(() => resolve(true), delay));
    }
    const connection: Connection = await client.connect(connectionString);
    
    const targetChannel: Channel = await connection.createChannel();
    await targetChannel.assertQueue(targetQueueName);
    
    const sourceChannel: Channel = await connection.createChannel();
    const reply: client.Replies.AssertQueue = await sourceChannel.assertQueue(sourceQueueName);
    
    if(messageCount == 0 || messageCount > reply.messageCount) {
        messageCount = reply.messageCount;
    }
    for(let i: number = 0; i < messageCount; i++) {
        const result: boolean = await consumeMessage(getMessageHandler(targetChannel, targetQueueName), getMessageHandler(sourceChannel, sourceQueueName), delay);
        if(result == false) {
            break;
        }
    }
    await closeChannel(targetChannel);
    await closeChannel(sourceChannel);
    await closeConnection(connection);
    return;
})();

