prev=2048;
num=50000000;
for size in 256 512 1024 2048; do
	sed -i 's/1024 \* '$prev'/1024 \* '$size'/' atomic_node.h;
	make rdtsc;
	for threads in 1 2 4 8 16 28 56; do
		for i in 1; do 
			echo "thread $threads" >> out/rdtsc/spinlock_innode_${size}
			./bin/rdtsc_spinlock_innode $num $threads >> out/rdtsc/spinlock_innode_${size} 
#			echo "thread $threads" >> out/rdtsc/mutex_${size}
#			./bin/rdtsc_finger $num $threads >> out/rdtsc/mutex_${size} 
#			echo "thread $threads" >> out/rdtsc/spinlock_${size}
#			./bin/rdtsc_spinlock $num $threads >> out/rdtsc/spinlock_${size} 
		done
	done
	prev=$size;
done


