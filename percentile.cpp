#include <iostream>
#include <vector>
#include <sys/time.h>
using namespace std;
#define MAXINT 0xffffffff

#define rotl(r,n) (((r)<<(n)) | ((r)>>((8*sizeof(r))-(n))))

typedef unsigned uint32_t;
uint32_t x,y,z;
float U01();
struct Record{
		long date;
		int sno;
		float val;
	};
int main(int argc, char *argv[]){
	
	
	int serialNo = 1;
	vector<Record> message;
	struct timeval start,end,currentTime,prograrmEndTime;
	long endTime;
	
	gettimeofday(&start,NULL);
	endTime = start.tv_usec+60*1000000+start.tv_sec*1000000;
	gettimeofday(&end,NULL);
	while((end.tv_usec+end.tv_sec*1000000)<endTime){
		//double rndValue = U01();
		
		for(int i =0;i< 50000; i++){
/*			Record record;
			gettimeofday(&currentTime,NULL);
			record.date = currentTime.tv_usec;
			record.sno = serialNo;
			record.val  = U01();
			message.push_back(record);
			*/
			serialNo++;
		}
		
		
		gettimeofday(&end,NULL);
	}
	gettimeofday(&prograrmEndTime,NULL);
	long finishTime = (-start.tv_sec+prograrmEndTime.tv_sec)*1000000 - start.tv_usec+prograrmEndTime.tv_usec;
	//printf("%ld ",finishTime);
	cout<<finishTime<<endl<<serialNo;
	return 0;
}

float U01() {  // Combined period = 2^95.999951
   x = ~(2911329625u*x); x = rotl(x,17); // CMFR, period=4294951751 (prime)
   y = 4031235431u * y;  y = rotl(y,15); // CMR,  period=4294881427 (prime)
   z = 3286325185u - rotl(z,19);         // CERS, period=4294921861=19*89*2539871
   return (float) (1.0*((x + y) ^ z)/MAXINT);
}
