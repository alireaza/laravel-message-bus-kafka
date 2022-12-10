<?php

namespace AliReaza\Laravel\MessageBus\Kafka\Handlers;

use AliReaza\MessageBus\HandlerInterface;
use AliReaza\MessageBus\MessageInterface;
use Symfony\Component\VarDumper\VarDumper;

class DumpHandler implements HandlerInterface
{
    public function __invoke(MessageInterface $message): void
    {
        VarDumper::dump($message);
    }
}
