%% @doc Almost the same as equery, but don't execute 'CLOSE'
%%
%% So, statement can be reused multiple times.
%% ```
%% > Bind
%% < BindComplete
%% > Execute
%% < DataRow*
%% < CommandComplete
%% > Sync
%% < ReadyForQuery
%% '''
-module(epgsql_cmd_prepared_query2).
-behaviour(epgsql_command).
-export([init/1, execute/2, handle_message/4]).
-export_type([response/0]).

-include("epgsql.hrl").
-include("protocol.hrl").

-type response() :: {ok, Count :: non_neg_integer(), Cols :: [epgsql:column()], Rows :: [tuple()]}
                  | {ok, Count :: non_neg_integer()}
                  | {ok, Cols :: [epgsql:column()], Rows :: [tuple()]}
                  | {error, epgsql:query_error()}.

-record(pquery2,
        {name :: iolist(),
         params :: list(),
         stmt = undefined :: #statement{} | undefined,
         decoder}).

init({Name, Parameters}) ->
    #pquery2{name = Name, params = Parameters}.

execute(Sock, #pquery2{name = Name, params = Params} = State) ->
    case maps:get(Name, epgsql_sock:get_stmts(Sock), undefined) of
        undefined ->
            Error = #error{
                severity = error,
                code = <<"26000">>,
                codename = invalid_sql_statement_name,
                message = list_to_binary(io_lib:format("prepared statement \"~s\" does not exist", [Name])),
                extra = []
            },
            {finish, {error, Error}, Sock};
        #statement{types = Types} = Stmt ->
            TypedParams = zip(Name, Types, Params),
            #statement{name = StatementName, columns = Columns} = Stmt,
            Codec = epgsql_sock:get_codec(Sock),
            Bin1 = epgsql_wire:encode_parameters(TypedParams, Codec),
            Bin2 = epgsql_wire:encode_formats(Columns),
            Commands =
                [
                    epgsql_wire:encode_bind("", StatementName, Bin1, Bin2),
                    epgsql_wire:encode_execute("", 0),
                    epgsql_wire:encode_sync()
                ],
            {send_multi, Commands, Sock, State#pquery2{stmt = Stmt}}
    end.

zip(Name, Types, Params) ->
    case length(Types) =:= length(Params) of
        true ->
            lists:zip(Types, Params);
        false ->
            error(#{cause => "prepared_data_types_and_column_count_mismatch",
                    name => Name,
                    types => Types,
                    types_count => length(Types),
                    values_count => length(Params)
                   })
    end.

%% prepared query
handle_message(?BIND_COMPLETE, <<>>, Sock, #pquery2{stmt = Stmt} = State) ->
    #statement{columns = Columns} = Stmt,
    epgsql_sock:notify(Sock, {columns, Columns}), % Why do we need this?
    Codec = epgsql_sock:get_codec(Sock),
    Decoder = epgsql_wire:build_decoder(Columns, Codec),
    {noaction, Sock, State#pquery2{decoder = Decoder}};
handle_message(?DATA_ROW, <<_Count:?int16, Bin/binary>>,
               Sock, #pquery2{decoder = Decoder} = State) ->
    Row = epgsql_wire:decode_data(Bin, Decoder),
    {add_row, Row, Sock, State};
handle_message(?EMPTY_QUERY, _, Sock, State) ->
    {add_result, {ok, [], []}, {complete, empty}, Sock, State};
handle_message(?COMMAND_COMPLETE, Bin, Sock, #pquery2{stmt = Stmt} = State) ->
    Complete = epgsql_wire:decode_complete(Bin),
    #statement{columns = Cols} = Stmt,
    Rows = epgsql_sock:get_rows(Sock),
    Result = case Complete of
                 {_, Count} when Cols == [] ->
                     {ok, Count};
                 {_, Count} ->
                     {ok, Count, Cols, Rows};
                 _ ->
                     {ok, Cols, Rows}
             end,
    {add_result, Result, {complete, Complete}, Sock, State};
handle_message(?READY_FOR_QUERY, _Status, Sock, _State) ->
    case epgsql_sock:get_results(Sock) of
        [Result] ->
            {finish, Result, done, Sock};
        [] ->
            {finish, done, done, Sock}
    end;
handle_message(?ERROR, Error, Sock, State) ->
    Result = {error, Error},
    {add_result, Result, Result, Sock, State};
handle_message(_, _, _, _) ->
    unknown.
