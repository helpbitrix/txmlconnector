@echo off
set CLICKHOUSE_URL=clickhouse://default:passwd@127.0.0.1:9000/default
set TC_LOGIN=TCNN9996
set TC_PASSWORD=w7G6yw
set TC_HOST=tr1-demo5.finam.ru
set TC_PORT=3939
set TC_TARGET=127.0.0.1:50051
set LOG_LEVEL=INFO
set EXPORT_SEC_INFO_NAMES=МТС
set EXPORT_SEC_BOARDS=TQBR,TQTF,FUT
set EXPORT_ALL_TRADES=positions,MTH5,MTZ4...

start exporter.exe