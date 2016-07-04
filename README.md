Simple AMQP Client
==================

Inspired by https://github.com/samrushing/amqp-shrapnel -- thanks Sam!

This library offers basic AMQP abstractions:  _connection_, _channel_, and
_consumer_.

On top of the basic abstractions, the RPC pattern is implemented as a part
of this library.

The library can be used to further build messaging patterns, such
as "publisher/subscriber", or "work queue".  More on messaging patterns,
please see:

- http://www.enterpriseintegrationpatterns.com/patterns/messaging/index.html

- https://www.rabbitmq.com/getstarted.html

- https://www.rabbitmq.com/how.html

The library is based on asynchronous event-driven threading model
_mrkthr_.

