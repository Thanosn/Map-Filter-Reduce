#include <time.h>
#include <stdlib.h>
#include <stdio.h>

int main(){
    srand(time(NULL));   

    int i =0;
    int r;
    FILE *fp = fopen("testfile", "w+");
    while (i++ <  1000000000/5){
        r = rand()%80000 + 10000;  

        fprintf(fp,"%d\n", r);
    }
    
    
}
