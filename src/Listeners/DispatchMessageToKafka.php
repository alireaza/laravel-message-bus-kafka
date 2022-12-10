<?php

namespace AliReaza\Laravel\MessageBus\Kafka\Listeners;

use AliReaza\Laravel\MessageBus\Kafka\Events\MessageCreated;
use AliReaza\MessageBus\Kafka\Helper as KafkaHelper;
use AliReaza\MessageBus\Kafka\MessageDispatcher as KafkaDispatcher;
use AliReaza\MessageBus\MessageDispatcher;
use InvalidArgumentException;
use RdKafka;

class DispatchMessageToKafka
{
    public array $config = [];

    public function handle(MessageCreated $event): void
    {
        $config = config('message-bus-kafka');

        $this->validateConfig($config);

        $this->setConfig($config);

        $kafka_dispatcher = $this->getKafkaDispatcher();

        $kafka_dispatcher->setPartition($this->getPartition());

        $kafka_dispatcher->setMsgFlags($this->getMsgFlags());

        $kafka_dispatcher->setTimeoutMs($this->getTimeoutMs());

        $message_dispatcher = new MessageDispatcher($kafka_dispatcher);

        $message = $event->message;

        $message_dispatcher->dispatch($message);
    }

    protected function validateConfig(array $config): void
    {
        if (!array_key_exists('servers', $config) || empty($config['servers'])) {
            $this->invalid('servers');
        }

        if (!array_key_exists('producer', $config) || empty($config['producer'])) {
            $this->invalid('producer');
        }

        if (!array_key_exists('partition', $config['producer']) || empty($config['producer']['partition'])) {
            $this->invalid('producer.partition');
        }

        if (!array_key_exists('timeout', $config['producer']) || empty($config['producer']['timeout'])) {
            $this->invalid('producer.timeout');
        }

        if (!array_key_exists('msg_flags', $config['producer']) || empty($config['producer']['msg_flags'])) {
            $this->invalid('producer.msg_flags');
        }
    }

    protected function invalid(string $name): void
    {
        throw new InvalidArgumentException('The default `' . $name . '` is not configured in the `laravel-request-to-message-bus` config file.');
    }

    protected function setConfig(array $config): void
    {
        $this->config = $config;
    }

    protected function getConfig(): array
    {
        return $this->config;
    }

    protected function getKafkaDispatcher(): KafkaDispatcher
    {
        $kafka = $this->getKafka();

        return new KafkaDispatcher($kafka);
    }

    protected function getKafka(): RdKafka\Conf
    {
        $servers = $this->getServers();

        return KafkaHelper::conf($servers);
    }

    protected function getServers(): string
    {
        $config = $this->getConfig();

        return $config['servers'];
    }

    protected function getPartition(): int
    {
        $config = $this->getConfig();

        return $config['producer']['partition'];
    }

    protected function getMsgFlags(): int
    {
        $config = $this->getConfig();

        return $config['producer']['msg_flags'];
    }

    protected function getTimeoutMs(): int
    {
        $config = $this->getConfig();

        return $config['producer']['timeout'] * 1000;
    }
}
