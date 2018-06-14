-module(csvx).

%% API
-export([
  new_spec/0,
  new_spec_ex/0,
  new_spec/3,
  new_spec_ex/3,
  
  new_parser/0,
  new_parser_ex/0,
  new_parser/1,
  new_parser_ex/1,
  
  open_read/1,
  open_write/1,
  open_write_append/1,
  open_write_buf0/1,
  open_write_append_buf0/1,
  
  read_next_lines_bin/2,
  read_next_lines_bin/3,
  
  write_line_bins_n/3,
  write_line_bins/3,
  write_n_line_bins/3,
  write_n_lines_bins/3,
  
  close/1,
  
  new_spec_direct/3,
  quote_bins_iolist_reverse/3,
  quote_bin_iolist_reverse/3,
  parse_next/4
]).

-record(csvx_spec, {
  % common
  separator_byte :: byte(),
  quote_byte :: byte(),
  binary_pattern_all :: binary:cp(),
  
  % only for read
  
  % only for write
  new_line_bin :: binary()
%%  , binary_pattern_quote :: binary:cp()
}).

-define(CSVX_ENTER_R_BYTE, $\r).
-define(CSVX_ENTER_N_BYTE, $\n).

-define(CSVX_DEFAULT_SEPARATOR_BYTE, $,).
-define(CSVX_DEFAULT_QUOTE_BYTE, $").
-define(CSVX_DEFAULT_NEW_LINE_BIN, <<?CSVX_ENTER_R_BYTE, ?CSVX_ENTER_N_BYTE>>).




-define(CSVX_PARSER_STATE_PLAIN_START, 1).
-define(CSVX_PARSER_STATE_PLAIN_MIDDLE, 2).
-define(CSVX_PARSER_STATE_QUOTED, 3). % in quoted
-define(CSVX_PARSER_STATE_QUOTED_QUOTE, 4). % quoted quote - end quote or double quote
-define(CSVX_PARSER_STATE_PLAIN_R, 5). % on \r
-define(CSVX_PARSER_STATE_FINISH, -1). %


-define(CSVX_READ_BUF_SIZE_DEFAULT,
  16 * 1024
%%  1
).

-record(csvx_parser, {
  state = ?CSVX_PARSER_STATE_PLAIN_START,
  cur_field_rev = [],
  cur_line_rev = [],
  res_rev = [],
  
  spec :: #csvx_spec{},
  
  line_num = 1,
  is_last = false,
  
  buffer_szie = ?CSVX_READ_BUF_SIZE_DEFAULT
}).

-define(CSVX_PARSER_ERROR_QUOTE_IN_MIDDLE, <<"quote_in_field_middle">>).
-define(CSVX_PARSER_ERROR_AFTER_QUOTE_FINISH, <<"wrong_after_quote_finish">>).

-define(CSVX_IOLIST_FIELD(FieldIOList), iolist_to_binary(FieldIOList)).


%%def_separator_byte() ->
%%  ?CSVX_DEFAULT_SEPARATOR_BYTE.
%%
%%def_qoute_byte() ->
%%  ?CSVX_DEFAULT_QUOTE_BYTE.
%%
%%def_new_line_bin() ->
%%  ?CSVX_DEFAULT_SEPARATOR_BYTE.

-spec new_spec() -> {ok, #csvx_spec{}}.
new_spec() ->
  new_spec_direct(
    ?CSVX_DEFAULT_SEPARATOR_BYTE,
    ?CSVX_DEFAULT_QUOTE_BYTE,
    ?CSVX_DEFAULT_NEW_LINE_BIN
  ).

-spec new_spec_ex() -> #csvx_spec{}.
new_spec_ex() ->
  case new_spec() of
    {ok, CsvSpec} ->
      CsvSpec;
    Error ->
      throw(Error)
  end.

-spec new_spec_direct(byte(), byte(), binary()) -> {ok, #csvx_spec{}}.
new_spec_direct(SeparatorByte, QuoteByte, NewLineBin)
  when is_binary(NewLineBin) ->
  SeparatorBin = <<SeparatorByte>>,
  QuoteBin = <<QuoteByte>>,
  {ok, #csvx_spec{
    separator_byte = SeparatorByte,
    quote_byte = QuoteByte,
    new_line_bin = NewLineBin,
    
    binary_pattern_all = binary:compile_pattern([
      SeparatorBin,
      QuoteBin,
      <<?CSVX_ENTER_N_BYTE>>,
      <<?CSVX_ENTER_R_BYTE>>
    ])
%%    , binary_pattern_quote = binary:compile_pattern([
%%      QuoteBin
%%    ])
  }}.

-spec new_spec(any(), any(), binary()) -> {ok, #csvx_spec{}} | {csvx_error, binary()}.
new_spec(SeparatorChar, QuoteChar, NewLine) ->
  SeparatorByte = param_byte(SeparatorChar),
  QuoteByte = param_byte(QuoteChar),
  NewLineBin = param_bin(NewLine),
  if
    not is_integer(SeparatorByte) ->
%%      {csvx, Reason} = SeparatorByte,
      {csvx_error, <<"wrong_separator_char">>};
    
    not is_integer(QuoteByte) ->
%%      {error, Reason} = QuoteByte,
      {csvx_error, <<"wrong_quote_char">>};
    
    NewLineBin =/= <<?CSVX_ENTER_N_BYTE>>,
    NewLineBin =/= <<?CSVX_ENTER_R_BYTE, ?CSVX_ENTER_N_BYTE>>,
    NewLineBin =/= <<?CSVX_ENTER_R_BYTE>>
      ->
      {csvx_error, <<"wrong_new_line_bin">>};
    
    true ->
      new_spec_direct(SeparatorByte, QuoteByte, NewLineBin)
  end.

-spec new_spec_ex(any(), any(), binary()) -> #csvx_spec{}.
new_spec_ex(SeparatorChar, QuoteChar, NewLine) ->
  case new_spec(SeparatorChar, QuoteChar, NewLine) of
    {ok, CsvSpec} ->
      CsvSpec;
    Error ->
      throw(Error)
  end.


open_read(FileName) ->
  file:open(FileName, [read, raw, binary, read_ahead]).

open_write(FileName) ->
  file:open(FileName, [write, raw, delayed_write]).

open_write_append(FileName) ->
  file:open(FileName, [write, raw, delayed_write, append]).

open_write_buf0(FileName) ->
  file:open(FileName, [write, raw]).

open_write_append_buf0(FileName) ->
  file:open(FileName, [write, raw, append]).


% with enter after
-spec write_line_bins_n(file:io_device(), #csvx_spec{}, [binary()]) -> ok | {error, any()}.
write_line_bins_n(FileIo, Spec, Line)
  when is_list(Line) ->
  file:write(FileIo, lists:reverse(
    [Spec#csvx_spec.new_line_bin | quote_bins_iolist_reverse(Spec, Line, [])]
  )).

% without enter
-spec write_line_bins(file:io_device(), #csvx_spec{}, [binary()]) -> ok | {error, any()}.
write_line_bins(FileIo, Spec, Line)
  when is_list(Line) ->
  file:write(FileIo, lists:reverse(
    quote_bins_iolist_reverse(Spec, Line, [])
  )).

% with enter before
-spec write_n_line_bins(file:io_device(), #csvx_spec{}, [binary()]) -> ok | {error, any()}.
write_n_line_bins(FileIo, Spec, Line)
  when is_list(Line) ->
  file:write(FileIo, lists:reverse(
    quote_bins_iolist_reverse(Spec, Line, [Spec#csvx_spec.new_line_bin])
  )).


% with enter before
-spec write_n_lines_bins(file:io_device(), #csvx_spec{}, [[binary()]]) -> ok | {error, any()}.
write_n_lines_bins(FileIo, Spec, Lines) ->
  write_n_lines_bins(FileIo, Spec, Lines, []).

write_n_lines_bins(FileIo, Spec, [Line | TailLines], AccIolistRev) ->
  AccIolistRev2 = quote_bins_iolist_reverse(Spec, Line, [Spec#csvx_spec.new_line_bin | AccIolistRev]),
  write_n_lines_bins(FileIo, Spec, TailLines, AccIolistRev2);
write_n_lines_bins(FileIo, _Spec, [], AccIolistRev) ->
  file:write(FileIo, lists:reverse(AccIolistRev)).


-spec read_next_lines_bin(file:io_device(), #csvx_parser{}) -> {ok, #csvx_parser{}, list()} | eof | {error, any()} | {csvx_error_parser, binary(), integer(), list()}.
read_next_lines_bin(FileIo, Parser) ->
  read_next_lines_bin(FileIo, Parser, Parser#csvx_parser.spec).

-spec read_next_lines_bin(file:io_device(), #csvx_parser{}, #csvx_spec{}) -> {ok, #csvx_parser{}, list()} | eof | {error, any()} | {csvx_error_parser, binary(), integer(), list()}.
read_next_lines_bin(FileIo, Parser, Spec) ->
  case file:read(FileIo, Parser#csvx_parser.buffer_szie) of
    {ok, Bin} ->
      Res = parse_next(Spec, Parser, Bin, false),
      case Res of
        {ok, Parser2, []} ->
          read_next_lines_bin(FileIo, Parser2, Spec);
        _ ->
          Res
      end;
    eof ->
      parse_next(Spec, Parser, <<>>, true);
    {error, _} = Err ->
      Err
  end.

close(FileIo) ->
  file:close(FileIo).

%% GENERATOR %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

quote_bins_iolist_reverse(Spec, [Bin | Tail], AccRev) ->
  AccRev2 = quote_bin_iolist_reverse(Spec, Bin, AccRev),
  quote_bins_iolist_reverse__2(Spec, Tail, AccRev2);
quote_bins_iolist_reverse(_Spec, [], AccRev) ->
  AccRev.

quote_bins_iolist_reverse__2(Spec, [Bin | Tail], AccRev) ->
  AccRev2 = quote_bin_iolist_reverse(Spec, Bin, [Spec#csvx_spec.separator_byte | AccRev]),
  quote_bins_iolist_reverse__2(Spec, Tail, AccRev2);
quote_bins_iolist_reverse__2(_Spec, [], AccRev) ->
  AccRev.


quote_bin_iolist_reverse(Spec, Bin, AccRev) ->
  case binary:matches(Bin, Spec#csvx_spec.binary_pattern_all) of
    [] ->
      [Bin | AccRev];
    Founds when is_list(Founds) ->
%%      io:format("'~ts'=~tp~n", [Bin, Founds]),
      AccRev2 = [Spec#csvx_spec.quote_byte | AccRev],
      quote_bin_acc_iolist__1(Spec, Bin, AccRev2, Founds, 0)
  end.

quote_bin_acc_iolist__1(Spec, Bin, AccRev, [{Pos, _Len1} | Tail], Offset) ->
  PosOffset = Pos - Offset,
  <<BinOffset:PosOffset/binary, Byte, BinTail/binary>> = Bin,
  if
    Byte =:= Spec#csvx_spec.quote_byte ->
      % double quote char
      quote_bin_acc_iolist__1(Spec, BinTail,
        [Byte, Byte, BinOffset | AccRev], Tail, Offset + PosOffset + 1);
    true ->
      quote_bin_acc_iolist__1(Spec, Bin, AccRev, Tail, Offset)
  end;
quote_bin_acc_iolist__1(Spec, Bin, AccRev, [], _BinOffset) ->
  [Spec#csvx_spec.quote_byte, Bin | AccRev].

%% PARSER %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


% strong parser - quote can be at start and end of field (no spaces)

new_parser() ->
  {ok, #csvx_parser{
    spec = new_spec()
  }}.

new_parser_ex() ->
  case new_parser() of
    {ok, CsvParser} ->
      CsvParser;
    Error ->
      throw(Error)
  end.

new_parser(CsvSpec) ->
  case CsvSpec of
    #csvx_spec{} ->
      %todo: more verify
      {ok, #csvx_parser{
        spec = CsvSpec
      }};
    _ ->
      {csvx_error, <<"wrong_csv_spec">>}
  end.

new_parser_ex(CsvSpec) ->
  case new_parser(CsvSpec) of
    {ok, CsvParser} ->
      CsvParser;
    Error ->
      throw(Error)
  end.

-spec parse_next(#csvx_spec{}, #csvx_parser{}, binary(), boolean()) -> {ok, #csvx_parser{}, list()} | eof | {csvx_error_parser, binary(), integer(), list()}.
parse_next(_Spec, #csvx_parser{state = ?CSVX_PARSER_STATE_FINISH}, _Bin, _IsLast) ->
  eof;
parse_next(Spec, Parser, <<>> = Bin, IsLast) ->
  if
    not IsLast ->
      {ok, Parser, []};
    true ->
      parse_next__1(Spec, Parser#csvx_parser{is_last = IsLast}, Bin, [], 0)
  end;
parse_next(Spec, Parser, Bin, IsLast) ->
  Founds = binary:matches(Bin, Spec#csvx_spec.binary_pattern_all),
  Parser2 =
    if
      not IsLast ->
        Parser;
      true ->
        Parser#csvx_parser{is_last = IsLast}
    end,
  parse_next__1(Spec, Parser2, Bin, Founds, 0).

parser_error_res(CodeBin, Parser) ->
  PartLineBins = [], %todo
  {
    csvx_error_parser,
    CodeBin,
    Parser#csvx_parser.line_num,
    PartLineBins
  }.

% require #csvx_parser.state =/= ?CSVX_PARSER_STATE_FINISH
parse_next__1(#csvx_spec{quote_byte = ByteQ, separator_byte = ByteS} = Spec, #csvx_parser{state = PSt} = Parser, Bin, [{Pos, _Len1} | Tail], Offset) ->
  PosOffset = Pos - Offset,
  <<BinOffset:PosOffset/binary, Byte, BinTail/binary>> = Bin,
  case PSt of
    
    %%%%%%%%%%%%%
    ?CSVX_PARSER_STATE_PLAIN_START ->
      case Byte of
        
        ByteS ->
          FieldRes =
            if
              PosOffset > 0 ->
                BinOffset;
              true ->
                <<>>
            end,
          Line = [FieldRes | Parser#csvx_parser.cur_line_rev],
          Parser2 = Parser#csvx_parser{
            state = ?CSVX_PARSER_STATE_PLAIN_START,
            cur_line_rev = Line,
            cur_field_rev = []
          },
          parse_next__1(Spec, Parser2, BinTail, Tail, Offset + PosOffset + 1);
        
        ?CSVX_ENTER_N_BYTE ->
          FieldRes =
            if
              PosOffset > 0 ->
                BinOffset;
              true ->
                <<>>
            end,
          LineRes = lists:reverse([FieldRes | Parser#csvx_parser.cur_line_rev]),
          Parser2 = Parser#csvx_parser{
            state = ?CSVX_PARSER_STATE_PLAIN_START,
            cur_line_rev = [],
            cur_field_rev = [],
            res_rev = [LineRes | Parser#csvx_parser.res_rev],
            line_num = Parser#csvx_parser.line_num + 1
          },
          parse_next__1(Spec, Parser2, BinTail, Tail, Offset + PosOffset + 1);
        
        ?CSVX_ENTER_R_BYTE ->
          FieldRes =
            if
              PosOffset > 0 ->
                BinOffset;
              true ->
                <<>>
            end,
          LineRes = lists:reverse([FieldRes | Parser#csvx_parser.cur_line_rev]),
          Parser2 = Parser#csvx_parser{
            state = ?CSVX_PARSER_STATE_PLAIN_R,
            cur_line_rev = [],
            cur_field_rev = [],
            res_rev = [LineRes | Parser#csvx_parser.res_rev],
            line_num = Parser#csvx_parser.line_num + 1
          },
          parse_next__1(Spec, Parser2, BinTail, Tail, Offset + PosOffset + 1);
        
        ByteQ ->
          if
            PosOffset > 0 ->
              parser_error_res(?CSVX_PARSER_ERROR_QUOTE_IN_MIDDLE, Parser);
            true -> % PosOffset =:= 0
              Parser2 = Parser#csvx_parser{
                state = ?CSVX_PARSER_STATE_QUOTED
              },
              parse_next__1(Spec, Parser2, BinTail, Tail, Offset + 1)
          end
      end;
    
    %%%%%%%%%%%%%
    ?CSVX_PARSER_STATE_PLAIN_MIDDLE ->
      case Byte of
        
        ByteS ->
          FieldRes =
            if
              PosOffset > 0 ->
                lists:reverse([BinOffset | Parser#csvx_parser.cur_field_rev]);
              true ->
                lists:reverse(Parser#csvx_parser.cur_field_rev)
            end,
          Line = [?CSVX_IOLIST_FIELD(FieldRes) | Parser#csvx_parser.cur_line_rev],
          Parser2 = Parser#csvx_parser{
            state = ?CSVX_PARSER_STATE_PLAIN_START,
            cur_line_rev = Line,
            cur_field_rev = []
          },
          parse_next__1(Spec, Parser2, BinTail, Tail, Offset + PosOffset + 1);
        
        ?CSVX_ENTER_N_BYTE ->
          FieldRes =
            if
              PosOffset > 0 ->
                lists:reverse([BinOffset | Parser#csvx_parser.cur_field_rev]);
              true ->
                lists:reverse(Parser#csvx_parser.cur_field_rev)
            end,
          LineRes = lists:reverse([?CSVX_IOLIST_FIELD(FieldRes) | Parser#csvx_parser.cur_line_rev]),
          Parser2 = Parser#csvx_parser{
            state = ?CSVX_PARSER_STATE_PLAIN_START,
            cur_line_rev = [],
            cur_field_rev = [],
            res_rev = [LineRes | Parser#csvx_parser.res_rev],
            line_num = Parser#csvx_parser.line_num + 1
          },
          parse_next__1(Spec, Parser2, BinTail, Tail, Offset + PosOffset + 1);
        
        ?CSVX_ENTER_R_BYTE ->
          FieldRes =
            if
              PosOffset > 0 ->
                lists:reverse([BinOffset | Parser#csvx_parser.cur_field_rev]);
              true ->
                lists:reverse(Parser#csvx_parser.cur_field_rev)
            end,
          LineRes = lists:reverse([?CSVX_IOLIST_FIELD(FieldRes) | Parser#csvx_parser.cur_line_rev]),
          Parser2 = Parser#csvx_parser{
            state = ?CSVX_PARSER_STATE_PLAIN_R,
            cur_line_rev = [],
            cur_field_rev = [],
            res_rev = [LineRes | Parser#csvx_parser.res_rev],
            line_num = Parser#csvx_parser.line_num + 1
          },
          parse_next__1(Spec, Parser2, BinTail, Tail, Offset + PosOffset + 1);
        
        ByteQ ->
          parser_error_res(?CSVX_PARSER_ERROR_QUOTE_IN_MIDDLE, Parser)
      end;
    
    %%%%%%%%%%%%%
    ?CSVX_PARSER_STATE_QUOTED ->
      case Byte of
        ByteQ ->
          Field =
            if
              PosOffset > 0 ->
                [BinOffset | Parser#csvx_parser.cur_field_rev];
              true ->
                Parser#csvx_parser.cur_field_rev
            end,
          Parser2 = Parser#csvx_parser{
            state = ?CSVX_PARSER_STATE_QUOTED_QUOTE,
            cur_field_rev = Field
          },
          parse_next__1(Spec, Parser2, BinTail, Tail, Offset + PosOffset + 1);
        _ ->
          parse_next__1(Spec, Parser, Bin, Tail, Offset)
      end;
    
    %%%%%%%%%%%%%
    ?CSVX_PARSER_STATE_QUOTED_QUOTE ->
      if
        PosOffset > 0 ->
          parser_error_res(?CSVX_PARSER_ERROR_AFTER_QUOTE_FINISH, Parser);
        true -> % PosOffset =:= 0
          case Byte of
            
            ByteS ->
              FieldRes = lists:reverse(Parser#csvx_parser.cur_field_rev),
              Line = [?CSVX_IOLIST_FIELD(FieldRes) | Parser#csvx_parser.cur_line_rev],
              Parser2 = Parser#csvx_parser{
                state = ?CSVX_PARSER_STATE_PLAIN_START,
                cur_line_rev = Line,
                cur_field_rev = []
              },
              parse_next__1(Spec, Parser2, BinTail, Tail, Offset + 1);
            
            ?CSVX_ENTER_N_BYTE ->
              FieldRes = lists:reverse(Parser#csvx_parser.cur_field_rev),
              LineRes = lists:reverse([?CSVX_IOLIST_FIELD(FieldRes) | Parser#csvx_parser.cur_line_rev]),
              Parser2 = Parser#csvx_parser{
                state = ?CSVX_PARSER_STATE_PLAIN_START,
                cur_line_rev = [],
                cur_field_rev = [],
                res_rev = [LineRes | Parser#csvx_parser.res_rev],
                line_num = Parser#csvx_parser.line_num + 1
              },
              parse_next__1(Spec, Parser2, BinTail, Tail, Offset + 1);
            
            ?CSVX_ENTER_R_BYTE ->
              FieldRes = lists:reverse(Parser#csvx_parser.cur_field_rev),
              LineRes = lists:reverse([?CSVX_IOLIST_FIELD(FieldRes) | Parser#csvx_parser.cur_line_rev]),
              Parser2 = Parser#csvx_parser{
                state = ?CSVX_PARSER_STATE_PLAIN_R,
                cur_line_rev = [],
                cur_field_rev = [],
                res_rev = [LineRes | Parser#csvx_parser.res_rev],
                line_num = Parser#csvx_parser.line_num + 1
              },
              parse_next__1(Spec, Parser2, BinTail, Tail, Offset + 1);
            
            ByteQ ->
              Field = [ByteQ | Parser#csvx_parser.cur_field_rev],
              Parser2 = Parser#csvx_parser{
                state = ?CSVX_PARSER_STATE_QUOTED,
                cur_field_rev = Field
              },
              parse_next__1(Spec, Parser2, BinTail, Tail, Offset + 1)
          end
      end;
    
    %%%%%%%%%%%%%
    ?CSVX_PARSER_STATE_PLAIN_R ->
      if
        PosOffset =:= 0, Byte =:= ?CSVX_ENTER_N_BYTE ->
          Parser2 = Parser#csvx_parser{
            state = ?CSVX_PARSER_STATE_PLAIN_START
          },
          parse_next__1(Spec, Parser2, BinTail, Tail, Offset + 1);
        true ->
          %equal to ?CSVX_PARSER_STATE_PLAIN_START (can optimize Parser#csvx_parser.cur_line_rev=[] and Byte =:= ?CSVX_ENTER_N_BYTE in case
          case Byte of
            
            ByteS ->
              FieldRes =
                if
                  PosOffset > 0 ->
                    BinOffset;
                  true ->
                    <<>>
                end,
              Line = [FieldRes | Parser#csvx_parser.cur_line_rev],
              Parser2 = Parser#csvx_parser{
                state = ?CSVX_PARSER_STATE_PLAIN_START,
                cur_line_rev = Line,
                cur_field_rev = []
              },
              parse_next__1(Spec, Parser2, BinTail, Tail, Offset + PosOffset + 1);
            
            ?CSVX_ENTER_N_BYTE ->
              FieldRes =
                if
                  PosOffset > 0 ->
                    BinOffset;
                  true ->
                    <<>>
                end,
              LineRes = lists:reverse([FieldRes | Parser#csvx_parser.cur_line_rev]),
              Parser2 = Parser#csvx_parser{
                state = ?CSVX_PARSER_STATE_PLAIN_START,
                cur_line_rev = [],
                cur_field_rev = [],
                res_rev = [LineRes | Parser#csvx_parser.res_rev],
                line_num = Parser#csvx_parser.line_num + 1
              },
              parse_next__1(Spec, Parser2, BinTail, Tail, Offset + PosOffset + 1);
            
            ?CSVX_ENTER_R_BYTE ->
              FieldRes =
                if
                  PosOffset > 0 ->
                    BinOffset;
                  true ->
                    <<>>
                end,
              LineRes = lists:reverse([FieldRes | Parser#csvx_parser.cur_line_rev]),
              Parser2 = Parser#csvx_parser{
                state = ?CSVX_PARSER_STATE_PLAIN_R,
                cur_line_rev = [],
                cur_field_rev = [],
                res_rev = [LineRes | Parser#csvx_parser.res_rev],
                line_num = Parser#csvx_parser.line_num + 1
              },
              parse_next__1(Spec, Parser2, BinTail, Tail, Offset + PosOffset + 1);
            
            ByteQ ->
              if
                PosOffset > 0 ->
                  parser_error_res(?CSVX_PARSER_ERROR_QUOTE_IN_MIDDLE, Parser);
                true -> % PosOffset =:= 0
                  Parser2 = Parser#csvx_parser{
                    state = ?CSVX_PARSER_STATE_QUOTED
                  },
                  parse_next__1(Spec, Parser2, BinTail, Tail, Offset + 1)
              end
          end
      end
    
    %%%%%%%%%%%%%
  
  end;
parse_next__1(_Spec, #csvx_parser{is_last = IsLast} = Parser, Bin, [], _BinOffset) ->
  if
    not IsLast ->
      Res = lists:reverse(Parser#csvx_parser.res_rev),
      case Bin of
        <<>> ->
          {ok, Parser#csvx_parser{
            res_rev = []
          }, Res};
        _ ->
          State2OrRes =
            case Parser#csvx_parser.state of
              ?CSVX_PARSER_STATE_PLAIN_START ->
                ?CSVX_PARSER_STATE_PLAIN_MIDDLE;
              ?CSVX_PARSER_STATE_PLAIN_MIDDLE ->
                ?CSVX_PARSER_STATE_PLAIN_MIDDLE;
              ?CSVX_PARSER_STATE_QUOTED ->
                ?CSVX_PARSER_STATE_QUOTED;
              ?CSVX_PARSER_STATE_QUOTED_QUOTE ->
                parser_error_res(?CSVX_PARSER_ERROR_AFTER_QUOTE_FINISH, Parser);
              ?CSVX_PARSER_STATE_PLAIN_R ->
                ?CSVX_PARSER_STATE_PLAIN_MIDDLE
            end,
          if
            is_integer(State2OrRes) ->
              {ok, Parser#csvx_parser{
                state = State2OrRes,
                res_rev = [],
                cur_field_rev = [Bin | Parser#csvx_parser.cur_field_rev]
              }, Res};
            true ->
              State2OrRes
          end
      end;
    
    true ->
      FieldRes =
        case Bin of
          <<>> ->
            lists:reverse(Parser#csvx_parser.cur_field_rev);
          _ ->
            lists:reverse([Bin | Parser#csvx_parser.cur_field_rev])
        end,
      LineRes = lists:reverse([?CSVX_IOLIST_FIELD(FieldRes) | Parser#csvx_parser.cur_line_rev]),
      Res = lists:reverse([LineRes | Parser#csvx_parser.res_rev]),
      Parser2 = Parser#csvx_parser{
        state = ?CSVX_PARSER_STATE_FINISH,
        cur_field_rev = [],
        cur_line_rev = [],
        res_rev = []
      },
      {ok, Parser2, Res}
  end.



param_byte(Char) ->
  if
    is_integer(Char) ->
      if
        (Char >= 0), (Char =< 255) ->
          Char;
        true ->
          {error, <<"integer_not_byte">>}
      end;
    true ->
      {error, <<"type">>}
  end.

param_bin(String) ->
  if
    is_list(String) ->
      try
        list_to_binary(String)
      catch
        _:_ ->
          {error, <<"list_is_not_string">>}
      end;
    is_binary(String) ->
      String;
    true ->
      {error, <<"type">>}
  end.


