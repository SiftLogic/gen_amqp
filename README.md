gen_amqp
========

####License: MIT
####Copyright: 2012, 2013 SiftLogic LLC <http://siftlogic.com>

`gen_amqp` is a behaviour for easier AMQP connection handling.  It
extends the `gen_server` behaviour with the additional callbacks of
`handle_basic_deliver/3` and `handle_basic_cancel/2`.  In addition to
this there are several wrappers for setting up connections, exchanges,
and queues.  Also included are tools for polling and subscribing.

`gen_amqp` uses [RabbitMQ](http://www.rabbitmq.com/) as the AMQP
client, but the interface is written in a backend agnostic way and it
should be relatively straight forward to replace it with another 0-9-1
AMQP client implementation.  Or possibly even a similar solution such
as [ZeroMQ](http://zeromq.org/).

## Usage

The simplest way to use `gen_amqp` is to implement the behaviour
callbacks together with subscribing to a queue.

Add a call to `gen_amqp:connect` in the `init` function, store the
returned connection in `State` and add a `gen_amqp:disconnect` in
`terminate`.

After setting up a subscription using `gen_amqp:subscribe_nocreate/3`
every message from the amqp server will result in a call to
`handle_basic_deliver/3`.  Handle the message and ack using
`gen_amqp:ack/2` and that's about it.

## Functions



## Callbacks

A module implementing the `gen_amqp` behaviour, should implement all
the standard callbacks for a `gen_server`.  In addition to those, the
following callbacks are expected.

`handle_basic_deliver(Tag, Data, State)`

> Receive and process message containing `Data`.  After processing is
> completed you manually have to ack the message using
> `gen_amqp:ack(Connection, Tag)`.  This function should return the
> same result as
> [`gen_server:handle_cast/2`](http://www.erlang.org/doc/man/gen_server.html#Module:handle_cast-2).

> Types:

>> Return types are the same as `gen_server:handle_cast/2`

>>     Tag = opaque()
>>     Data = any()
>>     State = any()

`handle_basic_cancel(Tag, State)`

> For handling `basic.cancel` messages from the AMQP server.  This
> function should return the same result as
> [`gen_server:handle_cast/2`](http://www.erlang.org/doc/man/gen_server.html#Module:handle_cast-2).

> Types:

>> Return types are the same as `gen_server:handle_cast/2`

