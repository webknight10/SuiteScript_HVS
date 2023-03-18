/** HARSHVARDHAN /
 * @NApiVersion 2.x
 * @NScriptType MapReduceScript
 * @NModuleScope SameAccount
 */
define(['N/record', 'N/runtime', 'N/search', 'N/file'],

function(record, runtime, search, file) {
   
    /**
     * Marks the beginning of the Map/Reduce process and generates input data.
     *
     * @typedef {Object} ObjectRef
     * @property {number} id - Internal ID of the record instance
     * @property {string} type - Record type id
     *
     * @return {Array|Object|Search|RecordRef} inputSummary
     * @since 2015.1
     */
    function getInputData() {
    	try{
    		var openArrayList = [];
    		var invoiceSearchObj = search.create({
    			   type: "invoice",
    			   filters:
    			   [
    			      ["type","anyof","CustInvc"], 
    			      "AND", 
    			      ["mainline","is","T"], 
    			      "AND", 
    			      ["name","anyof","25804"], 
    			      "AND", 
    			      ["status","anyof","CustInvc:A"]
    			   ],
    			   columns:
    			   [
    			      search.createColumn({name: "tranid", label: "Document Number"}),
    			      search.createColumn({name: "entity", label: "Name"}),
    			      search.createColumn({name: "amount", label: "Amount"}),
    			      search.createColumn({name: "amountpaid", label: "Amount Paid"}),
    			      search.createColumn({name: "email", label: "Email"})
    			   ]
    			});
    			var searchResultCount = invoiceSearchObj.runPaged().count;
    			log.debug("invoiceSearchObj result count",searchResultCount);
    			
    			var invTranId;
    			var custName;
				var invAmount;
				var invAmtPain;
				var invEmail;
								
    			invoiceSearchObj.run().each(function(result){
    				
    				invTranId = result.getValue('tranid');
    				log.debug('invTranId',invTranId);
    				
    				custName = result.getValue('entity');
    				log.debug('custName',custName);
    				
    				invAmount = result.getValue('amount');
    				log.debug('invAmount',invAmount);
    				
    				invAmtPain = result.getValue('amountpaid');
    				log.debug('invAmtPain',invAmtPain);
    				
    				invEmail = result.getValue('email');
    				log.debug('invEmail',invEmail);

    			   return true;
    			});
    			
    			openArrayList.push({
    				'invTranId':invTranId,
    				'custName':custName,
    				'invAmount':invAmount,
    				'invAmtPain':invAmtPain,
    				'invEmail':invEmail    				
    			});
    			
    			log.debug('openArrayList',openArrayList);
    			log.debug('End of getInputData');
    			return openArrayList;
    			
    	}catch(e){
    		log.error('Error In the getInputData',e);
    	}

    }
    /* 
     * End of The getInputData function
    */

    function map(context) {
    	try{
    		log.debug('In the Map Stage','In the Map Stage');
    		var invOutput = JSON.parse(context.value);
    		log.debug('invOutput',invOutput);
    		
    		var m_invTranId = invOutput.invTranId;
    		log.debug('m_invTranId',m_invTranId);
    		
    		var m_custName = invOutput.custName;
    		log.debug('m_custName',m_custName);
    		
			var m_invAmount = invOutput.invAmount;
			log.debug('m_invAmount',m_invAmount);
			
			var m_invAmtPain = invOutput.invAmtPain;
			log.debug('m_invAmtPain',m_invAmtPain);
			
			var m_invEmail = invOutput.invEmail;
			log.debug('m_invEmail',m_invEmail);
			
			context.write({
      			key : m_custName,
      			value :{
      				'm_custName':m_custName,
      				'm_invTranId':m_invTranId,
      				'm_invAmount':m_invAmount,
      				'm_invAmtPain':m_invAmtPain,
      				'm_invEmail':m_invEmail
      			}
      		});
	
    	}catch(e){
    		log.error('Error In the Map Function',e);
    		
    	};

    }

    
    function reduce(context) {

		try {
			var value = context.values;
			var value_Length = context.values.length;

			log.debug('In Reduce Stage');
			log.debug('Key:', context.key);
			log.debug('value:', context.values);
			log.debug('value Length:', context.values.length);
			
			var r_custName;
			var r_custTranid;
			var r_invAmount;
			var r_custEmail;
			
			
			
			var csvData = "";
			csvData = "Customer Name, Customer Transaction ID,Invoice Amount, Customer Email\n";
			
			
			var folderSearchObj = search.create({
				type: "folder",
				filters:[["internalid","anyof","13692"]],
                columns:[search.createColumn({name: "numfiles", label: "# of Files"})]		 
			});
			
			
			var existingFileCount = 0;
			var searchResultCount = folderSearchObj.runPaged().count;
			log.debug("folderSearchObj result count",searchResultCount);
			folderSearchObj.run().each(function(result){
				existingFileCount = result.getValue('numfiles');
				return true;
			});
			log.debug("existingFileCount",existingFileCount); //2
			var newFileCount = (Number(existingFileCount) + 1);
			log.debug("newFileCount",newFileCount); //3

			var fileName = 'Invoice Data HVS_'+newFileCount+'.csv';
			
			   
				    var csvFile = file.create({
						name : fileName,
						contents : csvData,
						folder : 13693,
						fileType : 'CSV'
					});
				    

				    for (var i = 0; i < searchResults.length; i++) {
				        var r_custName = searchResults[i].getValue({
				          name: 'entity'
				        });
				        log.debug('r_custName:',r_custName);
				        var r_custTranid = searchResults[i].getValue({
				          name: 'tranid'
				        });
				        log.debug('r_custTranid:',r_custTranid);
				        var r_invAmount = searchResults[i].getValue({
					          name: 'amount'
					        });
				        var r_custEmail = searchResults[i].getValue({
					          name: 'email'
					        });
				        var line =  r_custName + ',' + r_custTranid + ',' + r_invAmount + ',' + r_custEmail//+'\n';
				        
				        csvFile.appendLine({
				          value: line
				        });
				      }
				    
					 var csvFileId = csvFile.save();
						log.debug("file created succesfully");
			
			
				
		} catch (e) {
			log.error('Error in reduce', e);
		}

	}
    
    function summarize(summary) {

    }

    return {
        getInputData: getInputData,
        map: map,
        reduce: reduce,
        summarize: summarize
    };
    
});
