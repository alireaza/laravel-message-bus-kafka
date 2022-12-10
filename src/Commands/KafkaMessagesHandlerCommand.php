<?php

namespace AliReaza\Laravel\MessageBus\Kafka\Commands;

use AliReaza\Laravel\MessageBus\Kafka\Handlers\DumpHandler;
use AliReaza\MessageBus\Kafka\Helper as KafkaHelper;
use AliReaza\MessageBus\Kafka\MessageHandler as KafkaHandler;
use AliReaza\MessageBus\Message;
use AliReaza\MessageBus\MessageHandler;
use AliReaza\MessageBus\MessageHandlerInterface;
use AliReaza\MessageBus\MessageInterface;
use Illuminate\Console\Command;
use InvalidArgumentException;
use RdKafka;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputOption;

class KafkaMessagesHandlerCommand extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'message-bus:kafka';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Handle messages from Apache Kafka.';

    protected array $config = [];

    /**
     * Configures the current command.
     */
    protected function configure()
    {
        $this->initConfig();

        $this->addInputTopic();

        $this->addInputPartition();

        $this->addInputGroup();

        $this->addInputRandomGroupName();

        $this->addInputOffset();

        $this->addInputDisableAutoCommit();

        $this->addInputServers();
    }

    protected function initConfig(): void
    {
        $config = config('message-bus-kafka');

        $this->validateConfig($config);

        $this->setConfig($config);
    }

    protected function validateConfig(array $config): void
    {
        if (!array_key_exists('servers', $config) || empty($config['servers'])) {
            $this->invalid('servers');
        }

        if (!array_key_exists('consumer', $config) || empty($config['consumer'])) {
            $this->invalid('consumer');
        }

        if (!array_key_exists('group', $config['consumer']) || empty($config['consumer']['group'])) {
            $this->invalid('consumer.group');
        }

        if (!array_key_exists('offset', $config['consumer']) || empty($config['consumer']['offset'])) {
            $this->invalid('consumer.offset');
        }
    }

    protected function invalid(string $name): void
    {
        throw new InvalidArgumentException('The default `' . $name . '` is not configured in the `laravel-request-to-message-bus` config file.');
    }

    protected function addInputTopic(): void
    {
        $this->addArgument(
            name: 'topic',
            mode: InputArgument::REQUIRED,
            description: 'A unique string that identifies the topic name this consumer subscribe messages.',
        );
    }

    protected function addInputPartition(): void
    {
        $this->addOption(
            name: 'partition',
            shortcut: 'p',
            mode: InputOption::VALUE_OPTIONAL | InputOption::VALUE_IS_ARRAY,
            description: 'A list of partitions that are the topic.',
        );
    }

    protected function addInputGroup(): void
    {
        $config = $this->getConfig();

        $this->addOption(
            name: 'group',
            shortcut: 'g',
            mode: InputOption::VALUE_OPTIONAL,
            description: 'A unique string that identifies the consumer group this consumer belongs to.',
            default: $config['consumer']['group'],
        );
    }

    protected function getConfig(): array
    {
        return $this->config;
    }

    protected function setConfig(array $config): void
    {
        $this->config = $config;
    }

    protected function addInputRandomGroupName(): void
    {
        $this->addOption(
            name: 'random-group-name',
            shortcut: 'r',
            mode: InputOption::VALUE_NONE,
            description: 'Generate a random name and add it to the group ID.',
        );
    }

    protected function addInputOffset(): void
    {
        $config = $this->getConfig();

        $this->addOption(
            name: 'offset',
            shortcut: 'o',
            mode: InputOption::VALUE_OPTIONAL,
            description: 'The auto offset reset consumer configuration defines how a consumer should behave when consuming from a topic partition when there is no initial offset. <comment>[values: "smallest", "earliest", "beginning", "largest", "latest", "end"]</comment>',
            default: $config['consumer']['offset'],
        );
    }

    protected function addInputDisableAutoCommit(): void
    {
        $this->addOption(
            name: 'disable-auto-commit',
            shortcut: 'd',
            mode: InputOption::VALUE_NONE,
            description: 'Disabling automatic and periodic commit offsets.',
        );
    }

    protected function addInputServers(): void
    {
        $config = $this->getConfig();

        $this->addOption(
            name: 'servers',
            shortcut: 's',
            mode: InputOption::VALUE_OPTIONAL,
            description: 'A comma-separated list of host and port pairs that are the addresses of the Kafka brokers in a `bootstrap` Kafka cluster that a Kafka client connects to initially to bootstrap itself.',
            default: $config['servers'],
        );
    }

    /**
     * Execute the console command.
     *
     * @return int
     */
    public function handle(): int
    {
        $kafka_handler = $this->getKafkaHandler();

        $partitions = $this->getPartitions();

        $kafka_handler->setPartitions($partitions);

        $group = $this->getGroup();

        if ($this->getRandomGroupName()) {
            $group .= '-' . microtime(true);

            $group .= '-' . sha1($group);
        }

        $kafka_handler->setGroupId($group);

        $offset = $this->getOffset();

        $kafka_handler->setAutoOffsetReset($offset);

        $disable_auto_commit = $this->getDisableAutoCommit();

        $kafka_handler->setAutoCommit(!$disable_auto_commit);

        $kafka_handler->setTimeoutMs(1000);

        $this->trap([SIGINT, SIGTERM], function () use ($kafka_handler): void {
            $kafka_handler->unsubscribe();
        });

        $message_handler = $this->getMessageHandler($kafka_handler);

        $this->handleMessage($message_handler);

        return self::SUCCESS;
    }

    protected function getKafkaHandler(): KafkaHandler
    {
        $kafka = $this->getKafka();

        $group = $this->getGroup();

        return new KafkaHandler($kafka, $group);
    }

    protected function getKafka(): RdKafka\Conf
    {
        $servers = $this->getServers();

        return KafkaHelper::conf($servers);
    }

    protected function getServers(): string
    {
        return $this->option('servers');
    }

    protected function getGroup(): string
    {
        return $this->option('group');
    }

    protected function getPartitions(): ?array
    {
        if (!$this->hasOption('partition') || empty($partitions = $this->option('partition'))) {
            return null;
        }

        $_partitions = [];

        foreach ($partitions as $partition) {
            $name = $this->getTopic();

            $offset = $this->getOffset();

            if (count($partition_offset = explode(':', $partition, 2)) === 2) {
                [$partition, $offset] = $partition_offset;

                if ($partition != (int)$partition) {
                    throw new RdKafka\Exception('Invalid value "' . $partition . '" for partition option. Partition must be of type integer.');
                }

                if (is_null($offset_int = $this->offsetStringToInt($offset))) {
                    if ($offset === 'null') {
                        $offset = null;
                    } elseif (is_numeric($offset)) {
                        $offset = (int)$offset;
                    } else {
                        throw new RdKafka\Exception('Invalid value "' . $offset . '" for partition "' . $partition . '". Offset must be of type integer.');
                    }
                } else {
                    $offset = $offset_int;
                }
            }

            $_partitions[] = new RdKafka\TopicPartition(topic: $name, partition: (int)$partition, offset: $offset);
        }

        return $_partitions;
    }

    protected function getTopic(): string
    {
        $topic = $this->argument('topic');

        return KafkaHelper::name($topic);
    }

    protected function getOffset(): int
    {
        $offset = $this->option('offset');

        $offset_int = $this->offsetStringToInt($offset);

        if (is_null($offset_int)) {
            throw new RdKafka\Exception('Invalid value "' . $offset . '" for configuration property "auto.offset.reset"');
        }

        return $offset_int;
    }

    protected function offsetStringToInt(string $offset): ?int
    {
        return match ($offset) {
            'smallest', 'earliest', 'beginning' => RD_KAFKA_OFFSET_BEGINNING,
            'largest', 'latest', 'end' => RD_KAFKA_OFFSET_END,
            default => null,
        };
    }

    protected function getRandomGroupName(): bool
    {
        return $this->option('random-group-name');
    }

    protected function getDisableAutoCommit(): bool
    {
        return $this->option('disable-auto-commit');
    }

    protected function getMessageHandler(MessageHandlerInterface $message_handler): MessageHandlerInterface
    {
        return new MessageHandler($message_handler);
    }

    protected function handleMessage(MessageHandlerInterface $message_handler): void
    {
        $messages = $this->getMessages();

        foreach ($messages as $message) {
            $handlers = $this->getHandlers($message);

            foreach ($handlers as $handler) {
                $message_handler->addHandler($message, $handler);
            }
        }

        $message_handler->handle($message ?? null);
    }

    protected function getMessages(): iterable
    {
        $name = $this->getTopic();

        $message = new Message(name: $name);

        return [$message];
    }

    protected function getHandlers(MessageInterface $message): iterable
    {
        if (empty($message->getName())) {
            return [];
        }

        $dump_handler = new DumpHandler();

        return [$dump_handler];
    }
}
