<?php

namespace AliReaza\Laravel\MessageBus\Kafka;

use Illuminate\Support\ServiceProvider;
use AliReaza\Laravel\MessageBus\Kafka\Commands\KafkaMessagesHandlerCommand;

class LaravelMessageBusKafkaServiceProvider extends ServiceProvider
{
    /**
     * Bootstrap services.
     *
     * @return void
     */
    public function boot(): void
    {
        $this->mergeConfigFrom(__DIR__ . DIRECTORY_SEPARATOR . 'config.php', 'message-bus-kafka');

        $this->commands(KafkaMessagesHandlerCommand::class);
    }
}
