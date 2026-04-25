#sudo likwid-perfctr -C 0-19 -g FLOPS_DP -t 0.1s &> flops_dp.txt & sudo likwid-perfctr -C 0-19 -g FLOPS_SP -t 0.1s &> flops_sp.txt & sudo likwid-perfctr -C 0-19 -g MEM -t 0.1s &> mem.txt & sudo likwid-perfctr -C 0-19 -g L2 -t 0.1s &> l2.txt & sudo likwid-perfctr -C 0-19 -g L3 -t 0.1s &> l3.txt

sudo likwid-perfctr -C 0-19 -f -g MEM -t 0.1s &> mem.txt
