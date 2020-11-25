def client
  @client ||= Aws::DynamoDB::Client.new(
    region: "us-west-2",
    access_key_id: "AKIAJXWX6X6EDY4XEAMA",
    secret_access_key: "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",
    endpoint: "http://localhost:8000"
  )
end