<?php

return [
    'servers' => env('MESSAGE_BUS_KAFKA_SERVERS', '127.0.0.1:9092'),
    'producer' => [
        'partition' => env('MESSAGE_BUS_KAFKA_PRODUCER_PARTITION', RD_KAFKA_PARTITION_UA),
        'timeout' => env('MESSAGE_BUS_KAFKA_PRODUCER_TIMEOUT', 10),
        'msg_flags' => env('MESSAGE_BUS_KAFKA_PRODUCER_MESSAGE_FLAGS', RD_KAFKA_MSG_F_BLOCK),
    ],
    'consumer' => [
        'offset' => env('MESSAGE_BUS_KAFKA_CONSUMER_OFFSET', 'latest'),
        'group' => env('MESSAGE_BUS_KAFKA_CONSUMER_GROUP', 'Message.Bus-AliReaza'),
    ],
];
