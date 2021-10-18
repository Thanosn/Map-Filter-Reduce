#include <time.h>
#include <stdlib.h>
#include <stdio.h>

int main(){
    srand(time(NULL));   
    char *array[20]= {"asd", "ff", "gg", "wp", "tsir", "lpc", "klfth", "ns", "sa", "wa", "wtf", "greee", "hrre", "dfgfgt", "lflflf", "assassin", "executor", "cleaner", "ripper","hit"};
    
    int i =0;
    int r;
    FILE *fp = fopen("testfile_string", "w+");
    while (i++ <  1000000000/5){
        r = rand()%20;  

        fprintf(fp,"%s\n", array[r]);
    }
    
    
}
