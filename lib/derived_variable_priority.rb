module DerivedVariablePriority
  # include DynamodbHelper
  def tsort_calculation
    table_name = "DATAFOUNDRYAI"
    begin
      result = Dynamodb.client.scan(table_name: table_name)
      return if result.items.empty?
      result_items = result.items
      priority_hash_builder(result_items)
      array_of_table_items = priority_array_builder(result_items)
      update_dv_table(array_of_table_items, table_name)
    rescue Aws::DynamoDB::Errors::ServiceError => e
      Rails.logger.info(e.message)
    end
  end
  def priority_hash_builder(result)
    priority_hash = Hash.new { |key, value| key[value] = [] }
    sort_obj_hash = {}
    result.each do |r|
      sort_obj_hash [r['dv_name']] = r['parameters']['source_key']
    end
    sortable_object = sort_obj_hash
    sorted_array = Bunny::Tsort.tsort(sortable_object)
    sorted_array.each_with_index do |sorted_sub_arr, index|
      sorted_sub_arr.each do |sa|
        priority_hash[sa] << index if result.map { |r| r['dv_name'] }.include?(sa)
      end
    end
  end
  def priority_array_builder(result)
    array_of_table_items = []
    result.each do |re|
      priority_hash.each do |k|
        next unless re['dv_name'] == k
        hash_of_table_items = {}
        hash_of_table_items['priority'] = priority_hash[k]
        hash_of_table_items['study_env_uuid'] = re['study_env_uuid']
        hash_of_table_items['dv_uuid'] = re['dv_uuid']
        array_of_table_items << hash_of_table_items
      end
    end
  end
  def update_dv_table(array_of_table_items, table_name)
    array_of_table_items.each do |ca|
      dynamodb.update_item({
                             table_name: table_name,
                             key: { 'study_env_uuid' => ca['study_env_uuid'], 'dv_uuid' => ca['dv_uuid'] },
                             update_expression: 'SET priority = :priority',
                             expression_attribute_values: { ':priority' => ca['priority'].join(' ') },
                           })
    end
  end
end