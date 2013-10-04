%%% @description
%%% Behaviour for amqp connection handling
%%% @copyright 2012, 2013 SiftLogic LLC http://siftlogic.com
-module(gen_amqp).
-author('Daniel Luna <daniel@lunas.se>').
-author('Kat Marchán <kzm@sykosomatic.org>').

-behaviour(gen_server).

%% Standard gen_server calls
-export([start/3, start/4, start_link/3, start_link/4, abcast/2, abcast/3,
         call/2, call/3, cast/2, enter_loop/3, enter_loop/4,
         enter_loop/5, multi_call/2, multi_call/3, multi_call/4, reply/2]).
%% This exposes implementation details :-(
-export([close_minor_channels/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% Debug support
-export([connection_counters/0]).

-record(gen_amqp_conn,
        {connection,
         main_channel,
         minor_channels = [],
         consumer_tags = []
        }).
-record(tag, {channel, tag}).

-opaque connection() :: #gen_amqp_conn{}.
-opaque message_tag() :: any().
-opaque channel_tag() :: any().
-type queue_name() :: binary() | {binary(), binary()}.

%% AMQP utility
-export([connect/1, connect/4, connect/5, disconnect/1,
         purge/2, q_declare/2, q_declare/3,
         delete/2,
         create_fanout/2, create_topic/2,
         subscribed_queue/2, subscribed_queue/3,
         get/2, write/3, write/4, ack/2, reject/2,
         subscribe_nocreate/3, unsubscribe_all/1
        ]).
-export_type([connection/0, message_tag/0, channel_tag/0]).

-include_lib("amqp_client/include/amqp_client.hrl").

%% Most of this is a copy/paste from gen_server.erl
-callback init(Args :: term()) ->
    {ok, State :: term()} | {ok, State :: term(), timeout() | hibernate} |
    {stop, Reason :: term()} | ignore.
-callback handle_call(Request :: term(), From :: {pid(), Tag :: term()},
                      State :: term()) ->
    {reply, Reply :: term(), NewState :: term()} |
    {reply, Reply :: term(), NewState :: term(), timeout() | hibernate} |
    {noreply, NewState :: term()} |
    {noreply, NewState :: term(), timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: term()} |
    {stop, Reason :: term(), NewState :: term()}.
-callback handle_cast(Request :: term(), State :: term()) ->
    {noreply, NewState :: term()} |
    {noreply, NewState :: term(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: term()}.
-callback handle_basic_deliver(MessageTag :: message_tag(), Request :: term(),
                               State :: term()) ->
    {noreply, NewState :: term()} |
    {noreply, NewState :: term(), timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: term()} |
    {stop, Reason :: term(), NewState :: term()}.
-callback handle_basic_cancel(ChannelTag :: channel_tag(), State :: term()) ->
    {noreply, NewState :: term()} |
    {noreply, NewState :: term(), timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: term()} |
    {stop, Reason :: term(), NewState :: term()}.
-callback handle_info(Info :: timeout() | term(), State :: term()) ->
    {noreply, NewState :: term()} |
    {noreply, NewState :: term(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: term()}.
-callback terminate(Reason :: (normal | shutdown | {shutdown, term()} |
                               term()),
                    State :: term()) ->
    term().
-callback code_change(OldVsn :: (term() | {down, term()}), State :: term(),
                      Extra :: term()) ->
    {ok, NewState :: term()} | {error, Reason :: term()}.

%% gen_server wrapping
start(Module, Args, Options) ->
    gen_server:start(?MODULE, {Module, Args}, Options).

start(ServerName, Module, Args, Options) ->
    gen_server:start(ServerName, ?MODULE, {Module, Args}, Options).

start_link(Module, Args, Options) ->
    gen_server:start_link(?MODULE, {Module, Args}, Options).

start_link(ServerName, Module, Args, Options) ->
    gen_server:start_link(ServerName, ?MODULE, {Module, Args}, Options).

abcast(X, Y) ->
    gen_server:abcast(X, Y).

abcast(X, Y, Z) ->
    gen_server:abcast(X, Y, Z).

call(X, Y) ->
    gen_server:call(X, Y).

call(X, Y, Z) ->
    gen_server:call(X, Y, Z).

multi_call(X, Y) ->
    gen_server:multi_call(X, Y).

multi_call(X, Y, Z) ->
    gen_server:multi_call(X, Y, Z).

multi_call(A, B, C, D) ->
    gen_server:multi_call(A, B, C, D).

cast(X, Y) ->
    gen_server:cast(X, Y).

enter_loop(X, Y, Z) ->
    gen_server:enter_loop(X, Y, Z).

enter_loop(X, Y, Z, A) ->
    gen_server:enter_loop(X, Y, Z, A).

enter_loop(X, Y, Z, A, B) ->
    gen_server:enter_loop(X, Y, Z, A, B).

reply(A, B) ->
    gen_server:reply(A, B).


init({Module, Args}) ->
    wrap_result(Module, Module:init(Args)).

handle_call(Request, From, {Module, State}) ->
    wrap_result(Module, Module:handle_call(Request, From, State)).

handle_cast(Request, {Module, State}) ->
    wrap_result(Module, Module:handle_cast(Request, State)).

terminate(Reason, {Module, State}) ->
    Module:terminate(Reason, State).

code_change(OldVsn, {Module, State}, Extra) ->
    case Module:code_change(OldVsn, State, Extra) of
        {ok, NewState} ->
            {ok, {Module, NewState}};
        {error, Reason} ->
            {error, Reason}
    end.

%% TODO - format_status    

handle_info({#'basic.deliver'{delivery_tag = Tag},
             #amqp_msg{payload = PayloadBin}},
            {Module, State}) ->
    Term = binary_to_term(PayloadBin),
    wrap_result(Module, Module:handle_basic_deliver(Tag, Term, State));
handle_info(#'basic.cancel'{consumer_tag = Tag}, {Module, State}) ->
    wrap_result(Module, Module:handle_basic_cancel(Tag, State));
handle_info(X, {Module, State}) ->
    wrap_result(Module, Module:handle_info(X, State)).

wrap_result(_Module, ignore) ->
    ignore;
wrap_result(Module, {ok, State}) ->
    {ok, {Module, State}};
wrap_result(Module, {ok, State, Timeout}) ->
    {ok, {Module, State}, Timeout};
wrap_result(_Module, {stop, Reason}) ->
    {stop, Reason};
wrap_result(Module, {stop, Reason, State}) ->
    {stop, Reason, {Module, State}};
wrap_result(Module, {stop, Reason, Reply, State}) ->
    {stop, Reason, Reply, {Module, State}};
wrap_result(Module, {reply, Reply, State}) ->
    {reply, Reply, {Module, State}};
wrap_result(Module, {reply, Reply, State, Timeout}) ->
    {reply, Reply, {Module, State}, Timeout};
wrap_result(Module, {noreply, State}) ->
    {noreply, {Module, State}};
wrap_result(Module, {noreply, State, Timeout}) ->
    {noreply, {Module, State}, Timeout}.

%% AMQP utilities
connect(Config) when is_list(Config), is_tuple(hd(Config)) ->
    Host = get_value(host, Config),
    VHost = get_value(virtual_host, Config),
    User = get_value(username, Config),
    Pass = get_value(password, Config),
    Port = get_value(port, Config, 5672),
    connect(Host, VHost, User, Pass, Port);
connect([Host, VHost, User, Pass]) ->
    connect(Host, VHost, User, Pass, 5672);
connect([Host,VHost,User,Pass,Port]) ->
    connect(Host, VHost, User, Pass, Port).

connect(Host, VHost, User, Pass) ->
    connect(Host, VHost, User, Pass, 5672).

connect(Host, VHost, User, Pass, Port) when is_binary(Host) ->
    connect(binary_to_list(Host), VHost, User, Pass, Port);
connect(Host, VHost, User, Pass, Port) when is_atom(Host) ->
    connect(atom_to_list(Host), VHost, User, Pass, Port);
connect(Host, VHost, User, Pass, Port) when is_list(VHost) ->
    connect(Host, list_to_binary(VHost), User, Pass, Port);
connect(Host, VHost, User, Pass, Port) when is_atom(VHost) ->
    connect(Host, atom_to_binary(VHost, latin1), User, Pass, Port);
connect(Host, VHost, User, Pass, Port) when is_list(User) ->
    connect(Host, VHost, list_to_binary(User), Pass, Port);
connect(Host, VHost, User, Pass, Port) when is_atom(User) ->
    connect(Host, VHost, atom_to_binary(User, latin1), Pass, Port);
connect(Host, VHost, User, Pass, Port) when is_list(Pass) ->
    connect(Host, VHost, User, list_to_binary(Pass), Port);
connect(Host, VHost, User, Pass, Port) when is_atom(Pass) ->
    connect(Host, VHost, User, atom_to_binary(Pass, latin1), Port);
connect(Host0, VHost, User, Pass, Port) ->
    Host = case inet_parse:address(Host0) of
               {ok, Ip} -> Ip;
               {error, einval} -> Host0
           end,
    Params = #amqp_params_network{host = Host,
                                  virtual_host = VHost,
                                  username = User,
                                  password = Pass,
                                  port = Port},
    count_connect(),
    try amqp_connection:start(Params) of
        {ok, Connection} ->
            link(Connection),
            {ok, Channel} = amqp_connection:open_channel(Connection),
            %% Because of stupid
            amqp_selective_consumer:register_default_consumer(Channel, self()),
            #gen_amqp_conn{connection = Connection, main_channel = Channel}
    catch T:E ->
            error_logger:info_msg("Connection failure using ~p", [Params]),
            erlang:raise(T, E, erlang:get_stacktrace())
    end.

disconnect(#gen_amqp_conn{
              main_channel = Channel, connection = Connection}) ->
    count_disconnect(),
    %% Close the channel
    amqp_channel:close(Channel),
    %% Close the connection
    unlink(Connection),
    amqp_connection:close(Connection),
    ok.

count_connect() ->
    connection_counters(+1).

count_disconnect() ->
    connection_counters(-1).

connection_counters() ->
    ets:tab2list(gen_amqp_counters).

connection_counters(Count) ->
    catch throw(get_stacktrace),
    Mod = element(1, hd(tl(tl(erlang:get_stacktrace())))),
    update_counter(total, Count),
    update_counter(Mod, Count).

update_counter(Key, Count) ->
    try ets:update_counter(gen_amqp_counters, Key, Count), ok
    catch error:badarg ->
            try ets:insert_new(gen_amqp_counters, {Key, Count}) of
                true -> ok;
                false -> ets:update_counter(gen_amqp_counters, Key, Count), ok
            catch error:badarg ->
                    catch ets:new(
                            gen_amqp_counters,
                            [set,
                             public,
                             named_table,
                             {heir, whereis(application_controller), none}]),
                    update_counter(Key, Count)
            end
    end.


q_declare(AMQPConn, {Exchange, RoutingKey}) ->
    q_declare(AMQPConn, Exchange, RoutingKey).

q_declare(#gen_amqp_conn{main_channel = Channel} = AMQPConn,
          Exchange,
          RoutingKey)  ->
    #'queue.declare_ok'{queue = Q}
        = amqp_channel:call(Channel, #'queue.declare'{queue = RoutingKey,
                                                      durable = true}),
    bind(AMQPConn, Q, Exchange, RoutingKey),
    {Exchange, Q}.

delete(AMQPConn, {_Exchange, Q}) ->
    delete(AMQPConn, Q);
delete(#gen_amqp_conn{connection = Connection}, Q) when is_binary(Q) ->
    {ok, Channel} = amqp_connection:open_channel(Connection),
    try amqp_channel:call(Channel, #'queue.delete'{queue = Q}) of
        #'queue.delete_ok'{} -> amqp_channel:close(Channel), ok
    catch _:_ -> {error, no_such_queue}
    end.

purge(#gen_amqp_conn{main_channel = Channel}, Q) ->
    #'queue.purge_ok'{} =
        amqp_channel:call(Channel, #'queue.purge'{queue = Q}),
    ok.

-spec create_fanout(connection(), binary()) -> ok.
create_fanout(#gen_amqp_conn{main_channel = Channel}, {Name, _}) ->
    declare(Channel, <<"fanout">>, Name);
create_fanout(#gen_amqp_conn{main_channel = Channel}, Name) ->
    declare(Channel, <<"fanout">>, Name).

create_topic(#gen_amqp_conn{main_channel = Channel}, Name) ->
    declare(Channel, <<"topic">>, Name).

subscribed_queue(AmqpConn, {Exchange, RoutingKey}) ->
    subscribed_queue(AmqpConn, Exchange, RoutingKey).

subscribed_queue(#gen_amqp_conn{main_channel = Channel} = AmqpConn,
                 Exchange,
                 RoutingKey) ->
    #'queue.declare_ok'{queue = Queue} =
        amqp_channel:call(Channel, #'queue.declare'{
                            queue = RoutingKey,
                            auto_delete = true}),
    bind(AmqpConn, Queue, Exchange, RoutingKey),
    subscribe_nocreate(AmqpConn, Queue, 1).

declare(Channel, Type, Name) ->
    Declare = #'exchange.declare'{exchange = Name, type = Type,
                                  durable = true},
    #'exchange.declare_ok'{} = amqp_channel:call(Channel, Declare),
    ok.

write(AmqpConn, {Exchange, RoutingKey}, Data) ->
    write(AmqpConn, Exchange, RoutingKey, Data).

write(#gen_amqp_conn{main_channel = Channel}, Exchange, RoutingKey, Data) ->
    Payload = term_to_binary(Data),
    Publish = #'basic.publish'{exchange = Exchange, routing_key = RoutingKey},
    amqp_channel:call(Channel, Publish, #amqp_msg{payload = Payload}).

-spec get(connection(), queue_name()) ->
                 {empty | no_such_queue | {Value :: any(), #tag{}},
                  connection()}.
get(AmqpConn, {_Exchange, Q}) when is_binary(Q) ->
    get(AmqpConn, Q);
get(#gen_amqp_conn{connection = Connection} = AmqpConn, Q)
  when is_binary(Q) ->
    case lists:keyfind(Q, 2, AmqpConn#gen_amqp_conn.minor_channels) of
        {Channel, Q} ->
            try amqp_channel:call(Channel, #'basic.get'{queue = Q}) of
                #'basic.get_empty'{} ->
                    {empty, AmqpConn};
                {#'basic.get_ok'{delivery_tag = Tag}, Content} ->
                    {{binary_to_term(Content#amqp_msg.payload),
                      #tag{channel = Channel, tag = Tag}}, AmqpConn}
            catch
                exit:{{shutdown, {server_initiated_close, 404, _}}, _} ->
                    %% Our channel died because of a missing queue.
                    %% Also see
                    %% http://old.nabble.com/Turning-consumers-on-off-in-Erlang-client-td29572514.html
                    NewMinor = lists:keydelete(Q, 2, AmqpConn#gen_amqp_conn.minor_channels),
                    {no_such_queue, AmqpConn#gen_amqp_conn{minor_channels = NewMinor}}
            end;
        false ->
            {ok, NewChannel} = amqp_connection:open_channel(Connection),
            NewAmqpConn = AmqpConn#gen_amqp_conn{minor_channels = [{NewChannel, Q}
                                                | AmqpConn#gen_amqp_conn.minor_channels]},
            get(NewAmqpConn, Q) %% recursive call
    end.

close_minor_channels(#gen_amqp_conn{minor_channels = MinorChannels}) ->
    _ = lists:map(fun({Channel, _}) -> catch amqp_channel:close(Channel) end,
                  MinorChannels),
    ok.

ack(#gen_amqp_conn{}, #tag{channel = Channel, tag = Tag}) ->
    amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag});
ack(#gen_amqp_conn{main_channel = Channel}, Tag) ->
    amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag}).


reject(#gen_amqp_conn{}, #tag{channel = Channel, tag = Tag}) ->
    amqp_channel:cast(Channel,
                      #'basic.reject'{delivery_tag = Tag, requeue = true});
reject(#gen_amqp_conn{main_channel = Channel}, Tag) ->
    amqp_channel:cast(Channel,
                      #'basic.reject'{delivery_tag = Tag, requeue = true}).

bind(#gen_amqp_conn{main_channel = Channel}, Q, Exchange, RoutingKey) ->
    Bind = #'queue.bind'{
      queue = Q, exchange = Exchange, routing_key = RoutingKey},
    #'queue.bind_ok'{} = amqp_channel:call(Channel, Bind),
    ok.

subscribe_nocreate(AmqpConn, {_Exchange, Q}, Prefetch) ->
    subscribe_nocreate(AmqpConn, Q, Prefetch);
subscribe_nocreate(AmqpConn = #gen_amqp_conn{main_channel = Channel},
                   Q,
                   Prefetch) when is_binary(Q) ->
    #'basic.qos_ok'{} =
        amqp_channel:call(Channel, #'basic.qos'{prefetch_count = Prefetch}),
    #'basic.consume_ok'{consumer_tag = Tag} =
        amqp_channel:subscribe(Channel,
                               #'basic.consume'{queue = Q},
                               self()),
    receive
        #'basic.consume_ok'{consumer_tag = Tag} -> ok
    after 1000 ->
            throw({failed_to_setup_queue_reader, Q})
    end,
    AmqpConn#gen_amqp_conn{consumer_tags = [{Q, Tag} | AmqpConn#gen_amqp_conn.consumer_tags]}.

unsubscribe(AmqpConn = #gen_amqp_conn{main_channel = Channel}, Tag) ->
    amqp_channel:call(Channel, #'basic.cancel'{consumer_tag = Tag}),
    receive
        %% XXX: Can the cancel return something else?  If so
        %% we will wait here forever.
        #'basic.cancel_ok'{consumer_tag = Tag} ->
            ok
    end,
    AmqpConn#gen_amqp_conn{
      consumer_tags =
          lists:keydelete(Tag, 2, AmqpConn#gen_amqp_conn.consumer_tags)}.

unsubscribe_all(AmqpConn = #gen_amqp_conn{consumer_tags = Tags}) ->
    lists:foreach(fun({_Q, Tag}) ->
                          unsubscribe(AmqpConn, Tag)
                  end,
                  Tags),
    AmqpConn#gen_amqp_conn{consumer_tags = []}.

get_value(Key, List) ->
    case lists:keyfind(Key, 1, List) of
        {Key, Value} -> Value;
        false ->
            error_logger:info_msg("Missing Config, ~p, ~p", [Key, List]),
            throw({missing_config, Key})
    end.

get_value(Key, List, Default) ->
    case lists:keyfind(Key, 1, List) of
        {Key, Value} -> Value;
        false -> Default
    end.
