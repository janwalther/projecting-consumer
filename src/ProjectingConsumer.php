<?php
namespace finance\Projector;

use Broadway\Domain\DateTime;
use Broadway\Domain\DomainMessage;
use Broadway\Domain\Metadata;
use Broadway\EventHandling\EventListener;
use finance\domain\model\queue\QueueableEvent;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Message\AMQPMessage;
use Webmozart\Assert\Assert;

class ProjectingConsumer
{
    /** @var AbstractConnection */
    private $connection;

    /** @var AMQPChannel[] */
    private $listeners = [];

    public function __construct(AbstractConnection $connection)
    {
        $this->connection = $connection;
    }

    /**
     * Process incoming request to generate pdf invoices and send them through
     * email.
     */
    public function listen(string $queueName, EventListener $projector)
    {
        Assert::classExists($queueName);
        Assert::implementsInterface($queueName, QueueableEvent::class);

        if (isset($this->listeners[$queueName])) {
            $this->listeners[$queueName]['handlers'][] = $projector;

            return;
        }

        $channel = $this->connection->channel();

        $channel->queue_declare(
            $queueName,    #queue
            false,              #passive
            true,               #durable, make sure that RabbitMQ will never lose our queue if a crash occurs
            false,              #exclusive - queues may only be accessed by the current connection
            false               #auto delete - the queue is deleted when all consumers have finished using it
        );

        /**
         * don't dispatch a new message to a worker until it has processed and
         * acknowledged the previous one. Instead, it will dispatch it to the
         * next worker that is not still busy.
         */
        $channel->basic_qos(
            null,   #prefetch size - prefetch window size in octets, null meaning "no specific limit"
            1,      #prefetch count - prefetch window in terms of whole messages
            null    #global - global=null to mean that the QoS settings should apply per-consumer, global=true to mean that the QoS settings should apply per-channel
        );

        /**
         * indicate interest in consuming messages from a particular queue. When they do
         * so, we say that they register a consumer or, simply put, subscribe to a queue.
         * Each consumer (subscription) has an identifier called a consumer tag
         */
        $channel->basic_consume(
            $queueName,
            #queue
            '',
            #consumer tag - Identifier for the consumer, valid within the current channel. just string
            false,
            #no local - TRUE: the server will not send messages to the connection that published them
            false,
            #no ack, false - acks turned on, true - off.  send a proper acknowledgment from the worker, once we're done with a task
            false,
            #exclusive - queues may only be accessed by the current connection
            false,
            #no wait - TRUE: the server will not respond to the method. The client should not wait for a reply method
            [$this, 'process'] #callback
        );

        $this->listeners[$queueName] = [
            'channel' => $channel,
            'handlers' => [$projector]
        ];
    }

    public function run()
    {
        $sockets = [];
        foreach ($this->listeners as $listener) {
            $sockets[] = $listener['channel']->getConnection()->getSocket();
        }

        while (true) {
            // copy sockets to another variable because stream_select modifies $readSockets
            $readSockets = $sockets;
            $writeSockets = null;
            $exceptSockets = null;
            $numberChangedSockets = stream_select($readSockets, $writeSockets, $exceptSockets, 5000);

            if ($numberChangedSockets > 0) {
                // Something arrived on a socket, check all channels for a new message.
                // Do so in a non-blocking way!
                foreach ($this->listeners as $listener) {
                    if (in_array($listener['channel']->getConnection()->getSocket(), $readSockets, true)) {
                        // This is non-blocking, since there is data ready on the socket for this channel.
                        $listener['channel']->wait();
                    }
                }
            }
        }

        foreach ($this->listeners as $listener) {
            $listener['channel']->close();
        }
    }

    private function handle(AMQPMessage $msg)
    {
        foreach ($this->listeners as $queueName => $listener) {
            if ($listener['channel']->getChannelId() === $msg->delivery_info['channel']) {
                foreach ($listener['handlers'] as $handler) {
                    /** @var EventListener $handler */
                    $handler->handle(
                        new DomainMessage('', 0, new Metadata(), $queueName::deserialize($msg->body), DateTime::now()));
                }
            }
        }
    }

    /**
     * process received request
     *
     * @param AMQPMessage $msg
     */
    public function process(AMQPMessage $msg)
    {
        $this->handle($msg);

        /** @var AMQPChannel $channel */
        $channel = $msg->delivery_info['channel'];
        /**
         * If a consumer dies without sending an acknowledgement the AMQP broker
         * will redeliver it to another consumer or, if none are available at the
         * time, the broker will wait until at least one consumer is registered
         * for the same queue before attempting redelivery
         */
        $channel->basic_ack($msg->delivery_info['delivery_tag']);
    }
}
