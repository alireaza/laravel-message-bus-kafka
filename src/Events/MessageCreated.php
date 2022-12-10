<?php

namespace AliReaza\Laravel\MessageBus\Kafka\Events;

use AliReaza\MessageBus\MessageInterface;
use Illuminate\Foundation\Events\Dispatchable;

class MessageCreated
{
    use Dispatchable;

    public function __construct(public MessageInterface $message)
    {
    }
}
