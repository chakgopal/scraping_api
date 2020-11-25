class Api::ApplicationController < ActionController::Base
  protect_from_forgery with: :null_session
  #respond_to :json

  

  

  protected

 

   def exception_handling(e)
    logger.error "exception_handling: #{e}"
    status = e.http_status rescue 404
    headers['msg'] = e.message
    render json: {status: status, :success => false, :status_msg => e.message}
  end

  

  private

  
end