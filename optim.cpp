#include <optim.h>

int factorial(int n){
  return (n == 1 || n == 0) ? 1 : factorial(n - 1) * n;
}

int* joinEnumeration(joinIndex* index, joinTree_node* root){
	int i, j, k;
	joinTree_node* curr=root;
	int** BestTree=malloc(index->numOfrels*sizeof(int*));
	if (BestTree==NULL) {
    	fprintf(stderr, "Malloc failed \n"); 
    	exit(-1);
	}
	for(i=0;i<index->numOfrels;i++){
		BestTree[i]=malloc((i+1)sizeof(int));
		if (BestTree[i]==NULL) {
	    	fprintf(stderr, "Malloc failed \n"); 
	    	exit(-1);
		}
	}
	BestTree[0]=0;
	for(i=1;i<index->numOfrels;i++){
		for(j=0;j<index->numOfrels;j++){
			for(k=0;k<index->numOfrels;k++){
				if((index->posOfJoins[j][k]==NULL)||(k==j)) continue;
			}
		}

	}

}

int main(void){

	int i, combinations;
	joinTree_node root;
	root.degree=0;
	root.path=NULL;
	root.parent=NULL;
	root.child=NULL;

	joinIndex index;
	index.numOfrels=4;
	index.posOfJoins=malloc(index.numOfrels*(sizeof(int*)));
	if (index.posOfJoins==NULL) {
    	fprintf(stderr, "Malloc failed \n"); 
    	exit(-1);
	}
	for(i=0;i<index.numOfrels;i++){
		index.posOfJoins[i]=malloc(index.numOfrels*sizeof(int));
		if (index.posOfJoins[i]==NULL) {
	    	fprintf(stderr, "Malloc failed \n"); 
	    	exit(-1);
		}
	}

	
	combinations=factorial(index.numOfrels)/((factorial(index.numOfrels-2))*2);
	index.relCombs=malloc(combinations*sizeof(int*));
	if (index.relCombs==NULL) {
    	fprintf(stderr, "Malloc failed \n"); 
    	exit(-1);
	}

	//edw prospathe na ftiaksw kati tetoio [0,1][0,2][0,3][1,2][1,3][2,3] pou einai oloi oi pihtanoi sindiasmoi xwris epanalipseis 
	//alla exei kaei o egkefalos mou opote den borw na grapse ti loopa tha ta dw avrio me katharotero mialo

	for(i=0;i<combinations;i++){
		while(j<index.numOfrels){
			index.relCombs[i][0]=stable_rel;
			index.relCombs[i][1]=
		}
	}

	index.joinBuckets=malloc(combinations*sizeof(pred*));
	if (index.joinBuckets==NULL) {
    	fprintf(stderr, "Malloc failed \n"); 
    	exit(-1);
	}

	//tha parw to paradeigma me relations 0,1,2,3 (pseutika rels) kai joins 0.0=2.0 1.0=2.2 1.3=0.2 3.1=1.1



}