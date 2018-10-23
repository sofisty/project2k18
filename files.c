#include "files.h"

int count_lines(FILE *fp){
	int ch=0;
	int lines=0;
	while(!feof(fp)){
	  ch = fgetc(fp);
	  if(ch == '\n'){
	    lines++;
	  }
	}
	lines++;
	return lines;
}

int store_file(FILE* fp,char* buff,int buff_size,tuple* rel_t,int lines){
	char* token;
	int i=0;
	while (fgets(buff,buff_size,fp)) {
		//printf("%s\n",buff);

		token= strtok(buff," ");
		if(token==NULL){ printf("Something's wrong.Check the datafile\n"); exit(-1);}
		rel_t[i].payload= atoi(token);

		token= strtok(NULL," ");
		if(token==NULL){ printf("Something's wrong.Check the datafile\n"); exit(-1);}
		rel_t[i].key= atoi(token);
		
		i++;
	}

	for(i=0;i<lines;i++){
		printf("%d	%d\n",rel_t[i].payload,rel_t[i].key);
	}
	return 0;
}