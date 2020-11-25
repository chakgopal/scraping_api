Rails.application.routes.draw do
  # For details on the DSL available within this file, see https://guides.rubyonrails.org/routing.html
  namespace :api, defaults: {format: :json} do
  	resources :derived_variabels
  	post 'create_derived_variables_table', to: 'derived_variabels#create_derived_variables_table'
  	post 'add_new_item_in_dvm_table', to: 'derived_variabels#add_new_item_in_dvm_table' 
  	get 'dv_sort_compatible_obj', to: 'derived_variabels#dv_sort_compatible_obj'
  	get 'show', to: 'derived_variabels#dv_show'
  	patch 'dv_update', to: 'derived_variabels#dv_update'
  	get 'dv_table_data', to: 'derived_variabels#dv_table_data'
  	get 'scrap_data', to: 'scrapings#scrap_data'
  end
end
