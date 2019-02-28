all: 
	./build.sh

clean:
	rm -rf bin
	rm -rf *.pprof
	rm -rf *.output
	rm -rf logs
	rm -rf diagnostic/
	rm -rf *.pid
	
