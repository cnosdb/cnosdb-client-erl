%%%-------------------------------------------------------------------
%% @doc cnosdb public API
%% @end
%%%-------------------------------------------------------------------

-module(cnosdb_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    cnosdb_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
