-module(cnosdb).

-export([
    start_client/1,
    is_alive/1,
    is_alive/2,
    write/2,
    write/3,
    write_async/3,
    write_async/4,
    stop_client/1
]).

-spec start_client(list()) ->
    {ok, Client :: map()} | {error, {already_started, Client :: map()}} | {error, Reason :: term()}.
start_client(Options0) ->
    Pool = proplists:get_value(pool, Options0),
    Client = #{
        pool => Pool
    },
    Options = lists:keydelete(pool, 1, Options0),
    case ehttpc_sup:start_pool(Pool, Options) of
        {ok, _} ->
            ClientOptions = http_clients_options(Options),
            {ok, maps:merge(ClientOptions, Client)};
        {error, {already_started, _}} ->
            ClientOptions = http_clients_options(Options),
            {error, {already_started, maps:merge(ClientOptions, Client)}};
        {error, Reason} ->
            {error, Reason}
    end.

-spec is_alive(Client :: map()) -> true | false.
is_alive(Client) ->
    is_alive(Client, false).

-spec is_alive(Client :: map(), ReturnReason :: boolean()) ->
    true | false | {false, Reason :: term()}.
is_alive(Client, ReturnReason) ->
    Path = "/api/v1/ping",
    Headers = [],
    try
        Worker = pick_worker(Client, ignore),
        case ehttpc:request(Worker, get, {Path, Headers}) of
            {ok, 200, _} ->
                true;
            {ok, 200, _, _} ->
                true;
            Return ->
                maybe_return_reason(Return, ReturnReason)
        end
    catch
        E:R:S ->
            log_or_return_reason(
                #{exception => E, reason => R, stacktrace => S},
                ReturnReason
            )
    end.

-spec write(Client, Points) -> ok | {error, term()} when
    Client :: map(),
    Points :: [Point],
    Point :: #{
        measurement => atom() | binary() | list(),
        tags => map(),
        fields => map(),
        timestamp => integer()
    }.
write(Client = #{path := Path, headers := Headers}, Points) ->
    try
        Request = {Path, Headers, influxdb_line:encode(Points)},
        do_write(pick_worker(Client, ignore), Request)
    catch
        E:R:S ->
            logger:error("[CnosDB] Encode ~0p failed: ~0p ~0p ~p", [Points, E, R, S]),
            {error, R}
    end.

-spec write(Client, Key, Points) -> ok | {error, term()} when
    Client :: map(),
    Key :: any(),
    Points :: [Point],
    Point :: #{
        measurement => atom() | binary() | list(),
        tags => map(),
        fields => map(),
        timestamp => integer()
    }.
write(Client = #{path := Path, headers := Headers}, Key, Points) ->
    try
        Request = {Path, Headers, influxdb_line:encode(Points)},
        do_write(pick_worker(Client, Key), Request)
    catch
        E:R:S ->
            logger:error("[CnosDB] Encode ~0p failed: ~0p ~0p ~p", [Points, E, R, S]),
            {error, R}
    end.

-spec write_async(Client, Points, {ReplayFun, Args}) -> {ok, pid()} | {error, term()} when
    Client :: map(),
    Points :: [Point],
    Point :: #{
        measurement => atom() | binary() | list(),
        tags => map(),
        fields => map(),
        timestamp => integer()
    },
    ReplayFun :: function(),
    Args :: list().
write_async(Client = #{path := Path, headers := Headers}, Points, {ReplayFun, Args}) ->
    try
        Request = {Path, Headers, influxdb_line:encode(Points)},
        do_aysnc_write(pick_worker(Client, ignore), Request, {ReplayFun, Args})
    catch
        E:R:S ->
            logger:error("[CnosDB] Encode ~0p failed: ~0p ~0p ~p", [Points, E, R, S]),
            {error, R}
    end.

-spec write_async(Client, Key, Points, {ReplayFun, Args}) -> ok | {error, term()} when
    Client :: map(),
    Key :: any(),
    Points :: [Point],
    Point :: #{
        measurement => atom() | binary() | list(),
        tags => map(),
        fields => map(),
        timestamp => integer()
    },
    ReplayFun :: function(),
    Args :: list().
write_async(Client = #{path := Path, headers := Headers}, Key, Points, {ReplayFun, Args}) ->
    try
        Request = {Path, Headers, influxdb_line:encode(Points)},
        do_aysnc_write(pick_worker(Client, Key), Request, {ReplayFun, Args})
    catch
        E:R:S ->
            logger:error("[CnosDB] Encode ~0p failed: ~0p ~0p ~p", [Points, E, R, S]),
            {error, R}
    end.

-spec stop_client(Client :: map()) -> ok | term().
stop_client(#{pool := Pool}) ->
    ehttpc_sup:stop_pool(Pool).

%%%-------------------------------------------------------------------
%%% internal function
%%%-------------------------------------------------------------------

http_clients_options(Options) ->
    Path = write_path(Options),
    Headers = header(Options),
    PoolType = proplists:get_value(pool_type, Options, random),
    #{
        path => Path,
        headers => Headers,
        pool_type => PoolType
    }.

write_path(Options) ->
    BasePath = "/api/v1/write",
    List0 = [
        {"db", database},
        {"tenant", tenant},
        {"precision", precision}
    ],
    FoldlFun =
        fun({K1, K2}, Acc) ->
            case proplists:get_value(K2, Options) of
                undefined -> Acc;
                Val -> [{K1, Val} | Acc]
            end
        end,
    List = lists:foldl(FoldlFun, [], List0),
    case length(List) of
        0 ->
            BasePath;
        _ ->
            BasePath ++ "?" ++ uri_string:compose_query(List)
    end.

header(Options) ->
    Username = proplists:get_value(username, Options, <<"">>),
    Password = proplists:get_value(password, Options, <<"">>),
    Base64Auth = base64:encode(<<Username/binary, ":", Password/binary>>),
    [
        {<<"Content-type">>, <<"application/x-www-form-urlencoded">>},
        {<<"Authorization">>, <<"Basic ", Base64Auth/binary>>}
    ].

maybe_return_reason({ok, ReturnCode, _}, true) ->
    {false, ReturnCode};
maybe_return_reason({ok, ReturnCode, _, Body}, true) ->
    {false, {ReturnCode, Body}};
maybe_return_reason({error, Reason}, true) ->
    {false, Reason};
maybe_return_reason(_, _) ->
    false.

log_or_return_reason(#{} = Reason, true) ->
    {false, Reason};
log_or_return_reason(#{exception := E, reason := R, stacktrace := S}, _) ->
    logger:error("[CnosDB] is_alive exception: ~0p ~0p ~0p", [E, R, S]),
    false.

do_write(Worker, {_Path, _Headers, _Data} = Request) ->
    try ehttpc:request(Worker, post, Request) of
        {ok, 200, _} ->
            ok;
        {ok, 200, _, _} ->
            ok;
        {ok, StatusCode, Reason} ->
            {error, {StatusCode, Reason}};
        {ok, StatusCode, Reason, Body} ->
            {error, {StatusCode, Reason, Body}};
        Error ->
            {error, Error}
    catch
        E:R:S ->
            logger:error("[CnosDB] http write fail: ~0p ~0p ~0p", [E, R, S]),
            {error, {E, R}}
    end.

do_aysnc_write(Worker, Request, ReplayFunAndArgs) ->
    ok = ehttpc:request_async(Worker, post, Request, 5000, ReplayFunAndArgs),
    {ok, Worker}.

pick_worker(#{pool := Pool, pool_type := hash}, Key) ->
    ehttpc_pool:pick_worker(Pool, Key);
pick_worker(#{pool := Pool}, _Key) ->
    ehttpc_pool:pick_worker(Pool).
