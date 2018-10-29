#include "files.h"

FILE* generate_file(FILE* fp,int* lines,char* filename){ //dimiourgei ena arxeio "filename" panw sta opoia tha vasistei to relation
	int i;
	*lines=rand() % 200000 + 1000; //exei metaksi 1000 kai 200000 eggrafwn

	fp=fopen(filename,"w"); //graffei sto arxeio eggrafes ths morfhs [row_id    value]
	for(i=1;i<=(*lines);i++){
		fprintf(fp, "%d",i );
		fprintf(fp, " ");
		fprintf(fp, "%d", rand() %20000000);
		fprintf(fp, "\n");
	}
	fclose(fp);
	return fp; //epistrefei ton filepointer

}

int count_lines(FILE *fp){ //metraei tis eggrafes enos arxeiou
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

int store_file(FILE* fp,char* buff,int buff_size,tuple* rel_t,int lines){ //pairnei ena arxeio kai apothikevei tis eggrafes tou ws tuples enos relation
	char* token;
	int i=0;
	while (fgets(buff,buff_size,fp)) { //pairnei grammi -grammi pou kathe grammi einai mia eggrafh
		//printf("%s\n",buff);
		token= strtok(buff," ");
		if(token==NULL){ printf("Something's wrong.Check the datafile\n"); exit(-1);}
		rel_t[i].payload= atoi(token);

		token= strtok(NULL," ");
		if(token==NULL){ printf("Something's wrong.Check the datafile\n"); exit(-1);}
		rel_t[i].key= atoi(token);
		
		i++;
	}

	//for(i=0;i<lines;i++){
	//	printf("%d	%d\n",rel_t[i].payload,rel_t[i].key);
	//}
	return 0;
}

//tha kleithoun mono an kleithei ws main h testing.c

FILE* generate_file1(FILE* fp,int* lines,char* filename){ //dimiourgei ena arxeio mono me key=1
	int i;
	*lines=rand() % 10000 + 1000; //exei metaksi 1000 kai 200000 eggrafwn

	fp=fopen(filename,"w"); //graffei sto arxeio eggrafes ths morfhs [row_id    value]
	for(i=1;i<=(*lines);i++){
		fprintf(fp, "%d",i );
		fprintf(fp, " ");
		fprintf(fp, "%d", 1);
		fprintf(fp, "\n");
	}
	fclose(fp);
	return fp; //epistrefei ton filepointer

}

FILE* generate_file2(FILE* fp,int* lines,char* filename){ //dimiourgei ena arxeio mono me perittous
	int i,key;
	*lines=rand() % 200000 + 1000; //exei metaksi 1000 kai 200000 eggrafwn

	fp=fopen(filename,"w"); //graffei sto arxeio eggrafes ths morfhs [row_id    value]
	for(i=1;i<=(*lines);i++){
		fprintf(fp, "%d",i);
		fprintf(fp, " ");
		key=rand() %20000000;
		while(key%2==0) key=rand() %20000000;
		fprintf(fp, "%d", key);
		fprintf(fp, "\n");
	}
	fclose(fp);
	return fp; //epistrefei ton filepointer

}

FILE* generate_file3(FILE* fp,int* lines,char* filename){ //dimiourgei ena arxeio mono me artious
	int i,key;
	*lines=rand() % 200000 + 1000; //exei metaksi 1000 kai 200000 eggrafwn

	fp=fopen(filename,"w"); //graffei sto arxeio eggrafes ths morfhs [row_id    value]
	for(i=1;i<=(*lines);i++){
		fprintf(fp, "%d",i);
		fprintf(fp, " ");
		key=rand() %20000000;
		while(key%2!=0) key=rand() %20000000;
		fprintf(fp, "%d", key);
		fprintf(fp, "\n");
	}
	fclose(fp);
	return fp; //epistrefei ton filepointer

}