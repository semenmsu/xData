#!/bin/bash
DATE=$1
#defs
python cli.py add $DATE fast.moex_fx.def.app.itubuntu.raw.7z
python cli.py add $DATE fast.moex_eq.def.app.itubuntu.raw.7z
python cli.py add $DATE fast.moex_forts.def.app.itubuntu.raw.7z
python cli.py add $DATE fast.moex_opt.def.app.itubuntu.raw.7z
python cli.py add $DATE fast.moex_board.def.app.itubuntu.raw.7z

#trades
python cli.py add $DATE fast.moex_fx.trades.pcap.itubuntu.raw.7z
python cli.py add $DATE fast.moex_eq.trades.pcap.itubuntu.raw.7z
python cli.py add $DATE fast.moex_forts.trades.pcap.itubuntu.raw.7z
python cli.py add $DATE fast.moex_opt.trades.pcap.itubuntu.raw.7z
python cli.py add $DATE fast.moex_board.trades.pcap.itubuntu.raw.7z

#stat
python cli.py add $DATE fast.moex_fx.stat.pcap.itubuntu.raw.7z
python cli.py add $DATE fast.moex_eq.stat.pcap.itubuntu.raw.7z
python cli.py add $DATE fast.moex_forts.stat.pcap.itubuntu.raw.7z
python cli.py add $DATE fast.moex_opt.stat.pcap.itubuntu.raw.7z
python cli.py add $DATE fast.moex_board.stat.pcap.itubuntu.raw.7z



