require 'open-uri'
require 'nokogiri'
require 'pry'
require 'neto_api_ruby'
require 'httparty'
urls = []
products = [] 
# product_hash = {}
html = open("https://www.kincrome.com.au/productdisplay.aspx?filterProduct=(RibbonFlag:eq:10)")
doc = Nokogiri::HTML(html)
description = doc.css('.widget-productlist-image').css('a')

description.each do |de|
  url = de.attribute('href').value
  urls << url
 end
# p urls

  urls.each do |ur|
   p_html = open("https://www.kincrome.com.au/#{ur}/")
   p_doc = Nokogiri::HTML(p_html)
   product_tile = p_doc.css("div.product-detail-title")
   product_tile.each do |product|
   	product_hash = {}
   	product_name = product.css("h1.widget-product-title.page-title").text
   	product_part_no = product.css("h4.subtitle.product-subtitle").text.split('.')[1]
   	product_img = p_doc.at('.product-detail-img')['src']
   	p product_img
   	# binding.pry
   	product_hash["SKU"] = product_part_no
    product_hash["Name"] = product_name
   	products << product_hash
   end
   # product_img = p_doc.css("div.product-detail-title")
   # products << p_doc.css('.widget-productlist-code').css('a')
  end 
  # p products
#   binding.pry
#    d_hash = {}
#    hsh = {}
#    # d_hash
#    d_hash['portable-toolkit'] = products
#    hsh['category'] = d_hash
  