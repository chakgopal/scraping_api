class Api::DerivedVariabelsController < Api::ApplicationController
 require "aws-sdk"
 before_action :require_params_for_update, only: [:dv_update]
	def create_derived_variables_table
		Aws.config.update({
							region: "us-west-2",
							access_key_id: "AKIAJXWX6X6EDY4XEAMA",
							secret_access_key: "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",
							endpoint: "http://localhost:8000"
						})

		dynamodb = Aws::DynamoDB::Client.new

		
params = {
		table_name: "Dumyy22",
		key_schema: [
			{
				attribute_name: "study_env_uuid",
		        key_type: "HASH"  #Partition key
			},
		    {
		        attribute_name: "dv_name",
		        key_type: "RANGE"  #Sort key 
		    }
		    
		    
		],
		attribute_definitions: [
		    {
		    	attribute_name: "study_env_uuid",
		        attribute_type: "S"
		    },
		    {
		        attribute_name: "dv_name",
		        attribute_type: "S"
		    }

		],
		provisioned_throughput: { 
		    read_capacity_units: 10,
		    write_capacity_units: 10
		}
		}
		begin
			result = dynamodb.create_table(params)
			puts "Created table. Status: " + 
			    result.table_description.table_status;

		rescue  Aws::DynamoDB::Errors::ServiceError => error
			puts "Unable to create table:"
			puts "#{error.message}"
		end
	end

	def add_new_item_in_dvm_table
		study_env_uuid = params[:study_env_uuid] if params[:study_env_uuid].present?
		snapshot_uuid = params[:snapshot_uuid] if params[:snapshot_uuid].present?
		source_key = params[:parameters]['source_key'] if params[:parameters]['source_key'].present?
		p "poooooooooo"
		p source_key

        dv_name = params[:dv_name] if params[:dv_name].present?
        dv_label = params[:dv_label] if params[:dv_label].present?
        priority = params[:priority] if params[:priority].present?

        Aws.config.update({
							region: "us-west-2",
							access_key_id: "AKIAJXWX6X6EDY4XEAMA",
							secret_access_key: "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",
							endpoint: "http://localhost:8000"
						})

		dynamodb = Aws::DynamoDB::Client.new
		table_name = "Dumyy22"



		item = {
					study_env_uuid: study_env_uuid,
					priority: priority,
					dv_name: dv_name,
					dv_label: dv_label,
					snapshot_uuid: snapshot_uuid,
					dv_uuid: params[:dv_uuid],
                    operation: params[:operation],
                    data_type: params[:data_type],
                    target_dataset: params[:target_dataset],
                    source_dataset: params[:source_dataset],
					created_at:  DateTime.now.to_s,
					updated_at:  DateTime.now.to_s,

					parameters: {
										source_key: source_key	
									}
					
    
				}
       

           result = dynamodb.query({
                              expression_attribute_values: {
                                ':partition_key_val' => params[:study_env_uuid],
                                 # ':source_key' => dv_name,
                              },
                              expression_attribute_names: {
                              	'#p'  => 'parameters',
                              },
                              key_condition_expression: 'study_env_uuid = :partition_key_val',
                              projection_expression: 'dv_name,#p.source_key',
                              # filter_expression: '(#p.source_key IN (:source_key))',
                              
                              table_name: table_name
                            }).items
		   
        # p "---------------------------"
        p result
        result.each do|re|
          if params[:parameters]['source_key'].include?(re['dv_name']) && re['parameters']['source_key'].include?(dv_name)
           p "-----------------------------------"
          raise StandardError.new "This is an exception"
          end
        end    
        # p "---------------------------"
		params = {
					table_name: table_name,
					item: item
				}

	    begin
			result = dynamodb.put_item(params)
			# puts result

		rescue  Aws::DynamoDB::Errors::ServiceError => error
			puts "Unable to add item:"
			puts "#{error}"
		end


	end

	def dv_concat_operation(source_key_1,source_key_2,delimeter)
		# source_key = source_key if source_key.present?
		source_key_1 = source_key_1 if source_key_1.present?
		source_key_2 = source_key_2 if source_key_2.present?
		delimeter = delimeter if delimeter.present?
		created_dv = "#{source_key_1}#{delimeter}#{source_key_2}"
		return created_dv
	end

	def built_partition_key(param1,param2)
	  p "#{param1}__#{param2}"
	end


  def built_sort_key(param1,param2)
	p "#{param1}__#{param2}"
   end



	def dv_sort_compatible_obj
		Aws.config.update({
							region: "us-west-2",
							access_key_id: "AKIAJXWX6X6EDY4XEAMA",
							secret_access_key: "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",
							endpoint: "http://localhost:8000"
						})

		dynamodb = Aws::DynamoDB::Client.new
		table_name = "Dumyy22"
		begin
  			result = dynamodb.scan(table_name: table_name)
			# if result.items == nil
			# 	puts 'Could not find result'
			# 	exit 0
			return if result.items.empty?

			
				result = result.items
				priority_hash = Hash.new{|h, k| h[k] = []}
				condition_array_for_update_table = []
				sort_obj_hash = {}
				# sortable_object = result.map { |r| sort_obj_hash[r["config_object"]["dv_name"]] = r["config_object"]["source_key"]; sort_obj_hash}.uniq.inject(:merge)
				result.each do|r|
					sort_obj_hash[r["dv_name"]] = Array(r["parameters"]["source_key"])
				end
				sortable_object = sort_obj_hash
				p "================================="
				# p sortable_object
				# binding.pry
				begin
				sorted_array = Bunny::Tsort.tsort(sortable_object)
				dervived_variable_list = result.map{|r| r["dv_name"]}
                # p "==================================="
                # p dervived_variable_list
                # p sorted_array
                sorted_array.each_with_index do|sorted_sub_arr,i|
                	sorted_sub_arr.each do|sa|
                		if dervived_variable_list.include?(sa)
                            priority_hash[sa] << i
         #                    dynamodb.update_item({
									# table_name: table_name,
									# key: {
									# 'dv_name' => sa
									# },
									# update_expression: 'ADD priorities :priority',
									# expression_attribute_values: {
									# ':priority' => Set.new([i])
									# }
									# })
                		end
                	end
                end
                # p "oooooooooooooo"
                # p result
                # p "iiiiiiiiiiiii"
                # p priority_hash
                result.each do|re|
                	priority_hash.each do|k,v|
                		if re["dv_name"] == k
                            condition_hash_for_update_table = {}
                			# condition_hash_for_update_table[k] << v
                			# condition_hash_for_update_table["id"] << re["id"]
                			condition_hash_for_update_table["priority"] = priority_hash[k]
                			condition_hash_for_update_table["snapshot_uuid"] = re["snapshot_uuid"]
                			condition_hash_for_update_table["study_env_uuid"] = re["study_env_uuid"]
                			condition_hash_for_update_table["dv_name"] = re["dv_name"]
                			condition_hash_for_update_table["dv_label"] = re["dv_label"]
                			condition_array_for_update_table << condition_hash_for_update_table
                		end
                	end
                end
                # p "kkkkkkkkkkkkkk"
                # p condition_array_for_update_table
                item  = []
                priority = []
                condition_array_for_update_table.each do|ca|
                	built_partition_key = built_partition_key(ca["study_env_uuid"],ca["snapshot_uuid"])
                	built_sort_key = built_sort_key(ca["dv_name"],ca["dv_label"])
                    priority << ca["priority"].join(" ").to_i
                	dynamodb.update_item({
									table_name: table_name,
									key: {
									'study_env_uuid_snapshot_uuid' => "#{ca["study_env_uuid"]}__#{ca["snapshot_uuid"]}",
									'dv_name_dv_label' => "#{ca["dv_name"]}__#{ca["dv_label"]}"
									},
									update_expression: 'SET priority = :priority',
									expression_attribute_values: {
									':priority' => (ca["priority"].join(" ")).to_f
									}
									})
                	dynamo_db_item = dynamodb.get_item({ key: {
                                 study_env_uuid_snapshot_uuid:  "#{ca["study_env_uuid"]}__#{ca["snapshot_uuid"]}",
                                 dv_name_dv_label: "#{ca["dv_name"]}__#{ca["dv_label"]}"
                               },
                                 table_name: table_name })

                	 
                	 item << dynamo_db_item.item.except(:study_env_uuid_snapshot_uuid,:dv_name_dv_label) 
                	 
                end
                 p "---------------------------------------"
                 p priority
                # p condition_array_for_update_table
                # new_result = dynamodb.scan(table_name: table_name)
                # new_result = new_result.items
                # p "============================================"
                # p new_result
                # p sorted_array	  
               p "ooooppppssstyyyyatatat"
                 p item
           
                p "---------------------------------------"
        # rescue Bunny::Tsort::CyclicGraphException  => error
        #  puts error.message
        #  Rails.logger.info(error.message)
        #  return nil
        end      
		rescue  Aws::DynamoDB::Errors::ServiceError => error
  			puts 'Unable to find result:'
  			puts error.message
		end
	    
	end

	def dv_show
		 Aws.config.update({
							region: "us-west-2",
							access_key_id: "AKIAJXWX6X6EDY4XEAMA",
							secret_access_key: "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",
							endpoint: "http://localhost:8000"
						})

		dynamodb = Aws::DynamoDB::Client.new

		params_list = params[:parameters].permit
		p_list = params.permit(params_list)
		# p params_list
		sort_key = p_list.slice(:dv_name,:dv_label).join('_')
		partition_key = p_list.slice(:study_env_uuid,:snapshot_uuid).join('_')

		table_name = "MEDIDATAVA"
		dynamo_db_item = Dynamodb.client.get_item({ key: {
                                 study_env_uuid_snapshot_uuid: partition_key,
                                 dv_name_dv_label: sort_key,
                               },
                                 table_name: table_name })
		p dynamo_db_item.item.except(:study_env_uuid_snapshot_uuid,:dv_name_dv_label)
	end

	def dv_update
	 Aws.config.update({
							region: "us-west-2",
							access_key_id: "AKIAJXWX6X6EDY4XEAMA",
							secret_access_key: "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",
							endpoint: "http://localhost:8000"
						})

	  dynamodb = Aws::DynamoDB::Client.new
	  table_name = "Dumyy22"
      # table_name = dynamodb_table_name('derived_variables')
      result = dynamodb.scan(table_name: table_name).items
      source_keys_list = result.map{|r| r['parameters']['source_keys']}.flatten.uniq
      # msg = 'Derived Varuable can not be updated as it is used for creation of other Derived Variables'
      # raise ApplicationController::GenericInvalidInputError, msg if source_keys_list.include?(params[:old_dv_name])
      derived_variable_item = get_item_for_old_dv_name(table_name, params).item
      derived_variable_item['dv_name'] = params[:new_dv_name]
      derived_variable_item['dv_label'] = params[:dv_label]  if params[:dv_label].present?
      # p derived_variable_item['study_env_uuid']
      # delete_item(table_name, params)
      # updated_dv_name = params[:new_dv_name]
      # updated_dv_label = nil
      
     
      # 	p "----------------------------"
      #   derived_variable_item['dv_name'] = params[:dv_label]
      # else
      #   updated_dv_label = derived_variable_item['dv_label']
      # end
      # p "-----------------"
      item = updated_build_item(derived_variable_item)
      p item
      # update_tble_item = updated_table_item.stringify_keys
      # p update_tble_item

      res = dynamodb.put_item({
                                 item: item,
                                 # return_consumed_capacity: TOTAL,
                                 table_name: table_name,
                               })
      p res
      p get_item_for_new_dv_name(table_name, params).item.slice('dv_name','dv_label')
    end



def get_item_for_old_dv_name(table_name, params)
	 Aws.config.update({
							region: "us-west-2",
							access_key_id: "AKIAJXWX6X6EDY4XEAMA",
							secret_access_key: "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",
							endpoint: "http://localhost:8000"
						})

	  dynamodb = Aws::DynamoDB::Client.new
	  table_name = "Dumyy22"
      old_dv_name = params[:old_dv_name]
      study_env_uuid = params[:study_environment_uuid]
      dynamodb.get_item({ key: {
                                 study_env_uuid: params[:study_environment_uuid],
                                 dv_name: params[:old_dv_name],
                               },
                                 table_name: table_name })
    end
    def delete_item(table_name, params)
    	 Aws.config.update({
							region: "us-west-2",
							access_key_id: "AKIAJXWX6X6EDY4XEAMA",
							secret_access_key: "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",
							endpoint: "http://localhost:8000"
						})

	  dynamodb = Aws::DynamoDB::Client.new
	  table_name = "Dumyy22"
      dynamodb.delete_item({ key: {
                                 study_env_uuid: params[:study_environment_uuid],
                                 dv_name: params[:old_dv_name],
                               },
                                 table_name: table_name })
    end
    def updated_build_item(params)
      {
        study_env_uuid: params['study_env_uuid'],
        dv_uuid: params['dv_uuid'],
        dv_name:params['dv_name'],
        dv_label: params['dv_label'],
        operation: params['operation'],
        data_type: params['data_type'],
        target_dataset: params['target_dataset'],
        source_dataset: params['source_dataset'],
        parameters: params['parameters'],
        priority: params['priority'],
        created_at: params['created_at'],
        updated_at: DateTime.now.to_s
      }
    end
    def get_item_for_new_dv_name(table_name, params)
    	 Aws.config.update({
							region: "us-west-2",
							access_key_id: "AKIAJXWX6X6EDY4XEAMA",
							secret_access_key: "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",
							endpoint: "http://localhost:8000"
						})

	  dynamodb = Aws::DynamoDB::Client.new
	  table_name = "Dumyy22"
	  new_dv_name = params[:old_dv_name]
      study_env_uuid = params[:study_environment_uuid]
      dynamodb.get_item({ key: {
                                 study_env_uuid: params[:study_environment_uuid],
                                 dv_name: params[:new_dv_name],
                               },
                                 table_name: table_name })
    end

	# def dv_update
	# 	 Aws.config.update({
	# 						region: "us-west-2",
	# 						access_key_id: "AKIAJXWX6X6EDY4XEAMA",
	# 						secret_access_key: "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",
	# 						endpoint: "http://localhost:8000"
	# 					})

	# 	dynamodb = Aws::DynamoDB::Client.new
	#     table_name = "Dumyy2"
        

	# 	old_dv_name = params[:old_dv_name]
	# 	new_dv_name = params[:new_dv_name]
	# 	dv_label = params[:dv_label]
	# 	study_env_uuid = params[:study_env_uuid]

	# 	# dynamo_db_item = Dynamodb.client.get_item({ key: {
 #  #                                study_env_uuid: study_env_uuid,
 #  #                                dv_name: old_dv_name,
 #  #                              },
 #  #                                table_name: table_name })  

 #  #       p dynamo_db_item       
	# 	table_data = dynamodb.scan(table_name: table_name).items
	# 	p table_data
	# 	dv_list = table_data.map{|t| t['parameters']['source_key']}.flatten.uniq
	# 	p dv_list
 #        if dv_list.include?(old_dv_name)
 #        	p "alredy present"
 #        else 
 #         item = dynamodb.get_item({ key: {
 #                                  study_env_uuid: study_env_uuid,
 #                                  dv_name: old_dv_name,
 #                                },
 #                                table_name: table_name }).item

 #         p "-------------------------"
 #         p item
 #          dynamodb.delete_item({
	# 								table_name: table_name,
	# 								key: {
	# 								'study_env_uuid' => params[:study_env_uuid],
	# 								'dv_name' => params[:old_dv_name]
	# 								}
	# 								})
         
 #         item2 = {
	# 				study_env_uuid: study_env_uuid,
	# 				priority: item['priority'],
	# 				dv_name: new_dv_name,
	# 				dv_label: dv_label,
	# 				parameters: {
	# 									source_key: item['parameters']['source_key']	
	# 								}
					
    
	# 			}
	# 	params = {
	# 				table_name: table_name,
	# 				item: item2
	# 			}		
	# 	dynamodb.put_item(params)


 #        result = dynamodb.get_item({ key: {
 #                                  study_env_uuid: study_env_uuid,
 #                                  dv_name: new_dv_name,
 #                                },
 #                                table_name: table_name }).item
 #        p result.slice('dv_name','dv_label')
 #        # p result.attributes.values_at(:dv_name,:dv_label)
 #        end	
	# end

	def dv_table_data
		Aws.config.update({
							region: "us-west-2",
							access_key_id: "AKIAJXWX6X6EDY4XEAMA",
							secret_access_key: "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",
							endpoint: "http://localhost:8000"
						})

		dynamodb = Aws::DynamoDB::Client.new
	    table_name = "Dumyy22"
	    table_data = dynamodb.scan(table_name: table_name).items
	    p '-----------------------'
	    p table_data
	end

	private 

	# def valid_params_for_update
	# 	params.permit(%i[study_environment_uuid dv_name format], derived_variable: {})
	# end
end