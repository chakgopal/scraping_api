require 'kimurai'

class SimpleSpider < Kimurai::Base
  @name = "simple_spider"
  @engine = :mechanize
  @start_urls = []
  @config = {}

  def parse(response, url:, data: {})
  end
end

SimpleSpider.crawl!