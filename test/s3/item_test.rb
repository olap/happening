require File.expand_path('../../test_helper', __FILE__)

class ItemTest < Test::Unit::TestCase
  context "An Happening::S3::Item instance" do

    setup do
      Happening::Log.level = Logger::ERROR
      @item = Happening::S3::Item.new('the-bucket', 'the-key',
        :aws_access_key_id => '123',
        :aws_secret_access_key => 'secret',
        :server => '127.0.0.1')

      @time = "Thu, 25 Feb 2010 10:00:00 GMT"
      Time.stubs(:now).returns(Time.parse(@time))
      #stub(:utc_httpdate => @time, :to_i => 99, :usec => 88))
    end

    context "validation" do
      should "require a bucket and a key" do
        assert_raise(ArgumentError) do
          item = Happening::S3::Item.new()
        end

        assert_raise(ArgumentError) do
          item = Happening::S3::Item.new('the-key')
        end

        assert_nothing_raised(ArgumentError) do
          item = Happening::S3::Item.new('the-bucket', 'the-key')
        end

      end

      should "not allow unknown options" do
        assert_raise(ArgumentError) do
          item = Happening::S3::Item.new('the-bucket', 'the-key',
            :aws_access_key_id => '123',
            :aws_secret_access_key => 'secret',
            :lala => 'lulul')
        end
      end

      should "check valid protocol" do
        assert_raise(ArgumentError) do
          item = Happening::S3::Item.new('the-bucket', 'the-key',
            :aws_access_key_id => '123',
            :aws_secret_access_key => 'secret',
            :protocol => 'lulul')
        end

        assert_nothing_raised do
          item = Happening::S3::Item.new('the-bucket', 'the-key',
            :aws_access_key_id => '123',
            :aws_secret_access_key => 'secret',
            :protocol => 'http')
        end

        assert_nothing_raised do
          item = Happening::S3::Item.new('the-bucket', 'the-key',
            :aws_access_key_id => '123',
            :aws_secret_access_key => 'secret',
            :protocol => 'https')
        end
      end
    end

    context "when building the item url" do
      should "build the full path out of the server, bucket, and key" do
        @item = Happening::S3::Item.new('the-bucketissoooooooooooooooooooooooooooooooooooooolonggggggggggggggggggggggggggggggggggg', 'the-key',
          :aws_access_key_id => '123',
          :aws_secret_access_key => 'secret',
          :server => '127.0.0.1')
        assert_equal "https://127.0.0.1:443/the-bucketissoooooooooooooooooooooooooooooooooooooolonggggggggggggggggggggggggggggggggggg/the-key", @item.url
      end

      should "use the DNS bucket name where possible" do
        @item = Happening::S3::Item.new('bucket', 'the-key',
          :aws_access_key_id => '123',
          :aws_secret_access_key => 'secret')
        assert_equal "https://bucket.s3.amazonaws.com:443/the-key", @item.url
      end
    end

    context "when getting an item" do

      should "call the on success callback" do
        stub_request(:get, 'https://bucket.s3.amazonaws.com:443/the-key').to_return(fake_response("data-here"))

        called = false
        data = nil
        on_success = Proc.new {|http| called = true, data = http.response}
        @item = Happening::S3::Item.new('bucket', 'the-key')

        EM.run do
          @item.get(:on_success => on_success)

          EM.assertions do
            assert called
            assert_equal "data-here\n", data
            assert_requested :get, 'https://bucket.s3.amazonaws.com:443/the-key', :times => 1
          end
        end        
      end

      should "support direct blocks" do
        stub_request(:get, 'https://bucket.s3.amazonaws.com:443/the-key').to_return(fake_response("data-here"))

        called = false
        data = nil

        @item = Happening::S3::Item.new('bucket', 'the-key')

        EM.run do
          @item.get do |http|
            called = true
            data = http.response
          end

          EM.assertions do
            assert called
            assert_equal "data-here\n", data
            assert_requested :get, 'https://bucket.s3.amazonaws.com:443/the-key', :times => 1
          end
        end
      end

      should "support stream blocks" do
        stub_request(:get, 'https://bucket.s3.amazonaws.com:443/the-key').to_return(fake_response("")).times(1)

        called = false
        data = ""
        @item = Happening::S3::Item.new('bucket', 'the-key')

        EM.run do
          response = @item.get
          response.stream do |chunk|
            called = true
            data << chunk
          end
          response.on_body_data "data-here"

          EM.assertions do
            assert called
            assert_equal "data-here\n", data
            assert_requested :get, 'https://bucket.s3.amazonaws.com:443/the-key', :times => 1
          end
        end
      end

      should "sign requests if AWS credentials are passend" do
        time = "Thu, 25 Feb 2010 12:06:33 GMT"
        Time.stubs(:now).returns(Time.parse(time))

        stub_request(:get, 'https://bucket.s3.amazonaws.com:443/the-key').
          with(:headers => {"Authorization"=>"AWS abc:3OEcVbE//maUUmqh3A5ETEcr9TE=", 'date' => time}).
          to_return(fake_response("data-here")).times(1)
        
        @item = Happening::S3::Item.new('bucket', 'the-key',
          :aws_access_key_id => 'abc',
          :aws_secret_access_key => '123')

        EM.run do
          @item.get

          EM.assertions do
            assert_requested :get, 'https://bucket.s3.amazonaws.com:443/the-key', :times => 1
          end
        end 
      end

      should "retry on error" do
        stub_request(:get, 'https://bucket.s3.amazonaws.com:443/the-key').to_return(error_response(400)).times(5)

        @item = Happening::S3::Item.new('bucket', 'the-key')
        EM.run do
          @item.get(:on_error => Proc.new{} ) #ignore error

          EM.assertions do
            assert_requested :get, 'https://bucket.s3.amazonaws.com:443/the-key', :times => 5
          end
        end
      end

      should "handle re-direct" do
        stub_request(:get, 'https://bucket.s3.amazonaws.com:443/the-key').
          to_return(redirect_response('https://bucket.s3-external-3.amazonaws.com/the-key')).times(1)
        stub_request(:get, 'https://bucket.s3-external-3.amazonaws.com:443/the-key').
          to_return(fake_response('hy there')).times(1)

        @item = Happening::S3::Item.new('bucket', 'the-key')
        
        EM.run do
          @item.get

          EM.assertions do
            assert_requested :get, 'https://bucket.s3.amazonaws.com:443/the-key', :times => 1
            assert_requested :get, 'https://bucket.s3-external-3.amazonaws.com:443/the-key', :times => 1
          end
        end
      end
    end

    context "when deleting an item" do
      should "send a DELETE to the items location" do
        stub_request(:delete, 'https://bucket.s3.amazonaws.com:443/the-key').
          with(:headers => {
            "Authorization"=>"AWS abc:nvkrlq4wor1qbFXZh6rHnAbiRjk=",
            'date' => @time,
            'url' => "/bucket/the-key"}).to_return(fake_response("data-here")).times(1)

        @item = Happening::S3::Item.new('bucket', 'the-key',
          :aws_access_key_id => 'abc',
          :aws_secret_access_key => '123')
        
        EM.run do
          @item.delete

          EM.assertions do
            assert_requested :delete, 'https://bucket.s3.amazonaws.com:443/the-key', :times => 1
          end
        end
      end

      should "support direct blocks" do
        stub_request(:delete, 'https://bucket.s3.amazonaws.com:443/the-key').
          with(:headers => {
            "Authorization"=>"AWS abc:nvkrlq4wor1qbFXZh6rHnAbiRjk=",
            'date' => @time,
            'url' => "/bucket/the-key"}).to_return(fake_response("data-here")).times(1)

        @item = Happening::S3::Item.new('bucket', 'the-key',
          :aws_access_key_id => 'abc',
          :aws_secret_access_key => '123')
        called = false
        data = nil
                
        EM.run do
          @item.delete do |http|
            called = true
            data = http.response
          end

          EM.assertions do
            assert called
            assert_equal "data-here\n", data
            assert_requested :delete, 'https://bucket.s3.amazonaws.com:443/the-key', :times => 1
          end
        end
      end

      should "handle re-direct" do
        stub_request(:delete, 'https://bucket.s3.amazonaws.com:443/the-key').
          with(:headers => {
            "Authorization"=>"AWS abc:nvkrlq4wor1qbFXZh6rHnAbiRjk=",
            'date' => @time,
            'url' => "/bucket/the-key"}).
          to_return(redirect_response('https://bucket.s3-external-3.amazonaws.com/the-key')).times(1)
        stub_request(:delete, 'https://bucket.s3-external-3.amazonaws.com:443/the-key').
          with({
            "Authorization"=>"AWS abc:nvkrlq4wor1qbFXZh6rHnAbiRjk=",
            'date' => @time,
            'url' => "/bucket/the-key"}).to_return(fake_response("success!")).times(1)

        @item = Happening::S3::Item.new('bucket', 'the-key',
          :aws_access_key_id => 'abc',
          :aws_secret_access_key => '123')
        
        EM.run do
          @item.delete

          EM.assertions do
            assert_requested :delete, 'https://bucket.s3.amazonaws.com:443/the-key', :times => 1
            assert_requested :delete, 'https://bucket.s3-external-3.amazonaws.com:443/the-key', :times => 1
          end
        end
      end

      should "handle retry" do
        stub_request(:delete, 'https://bucket.s3.amazonaws.com:443/the-key').
          with(:headers => {
            "Authorization"=>"AWS abc:nvkrlq4wor1qbFXZh6rHnAbiRjk=",
            'date' => @time,
            'url' => "/bucket/the-key"}).to_return(error_response(400)).times(5)

        @item = Happening::S3::Item.new('bucket', 'the-key',
          :aws_access_key_id => 'abc',
          :aws_secret_access_key => '123')
        
        EM.run do
          @item.delete(:on_error => Proc.new{} ) #ignore error

          EM.assertions do
            assert_requested :delete, 'https://bucket.s3.amazonaws.com:443/the-key', :times => 5
          end
        end
      end
    end

    context "when loading the headers" do
      should "request via HEAD" do
        stub_request(:head, 'https://bucket.s3.amazonaws.com:443/the-key').
          to_return(fake_response('hy there')).times(1)

        @item = Happening::S3::Item.new('bucket', 'the-key')
        EM.run do
          @item.head

          EM.assertions do
            assert_requested :head, 'https://bucket.s3.amazonaws.com:443/the-key', :times => 1
          end
        end
      end
    end

    context "when saving an item" do

      should "post to the desired location" do
        stub_request(:put, 'https://bucket.s3.amazonaws.com:443/the-key').
          with({
            "Authorization"=>"AWS abc:lZMKxGDKcQ1PH8yjbpyN7o2sPWg=",
            'date' => @time,
            'url' => "/bucket/the-key"}).to_return(fake_response("data-here")).times(1)

        @item = Happening::S3::Item.new('bucket', 'the-key',
          :aws_access_key_id => 'abc',
          :aws_secret_access_key => '123')
        
        EM.run do
          @item.put('content')

          EM.assertions do
            assert_requested :put, 'https://bucket.s3.amazonaws.com:443/the-key', :times => 1
          end
        end
      end

      should "support direct blocks" do
        stub_request(:put, 'https://bucket.s3.amazonaws.com:443/the-key').
          with(:headers => {
            "Authorization"=>"AWS abc:lZMKxGDKcQ1PH8yjbpyN7o2sPWg=",
            'date' => @time,
            'url' => "/bucket/the-key"}).to_return(fake_response("data-here")).times(1)

        called = false
        data = nil
        @item = Happening::S3::Item.new('bucket', 'the-key',
          :aws_access_key_id => 'abc',
          :aws_secret_access_key => '123')

        EM.run do
          @item.put('upload me') do |http|
            called = true
            data = http.response
          end

          EM.assertions do
            assert called
            assert_equal "data-here\n", data
            assert_requested :put, 'https://bucket.s3.amazonaws.com:443/the-key', :times => 1
          end
        end
      end

      should "set the desired permissions" do
        stub_request(:put, 'https://bucket.s3.amazonaws.com:443/the-key').
          with(:headers => {
            "Authorization"=>"AWS abc:cqkfX+nC7WIkYD+yWaUFuoRuePA=",
            'date' => @time,
            'url' => "/bucket/the-key",
            "x-amz-acl" => 'public-read'}).to_return(fake_response("data-here")).times(1)

        @item = Happening::S3::Item.new('bucket', 'the-key',
          :aws_access_key_id => 'abc',
          :aws_secret_access_key => '123' ,
          :permissions => 'public-read')
        
        EM.run do
          @item.put('content')

          EM.assertions do
            assert_requested :put, 'https://bucket.s3.amazonaws.com:443/the-key', :times => 1
          end
        end
      end

      should "allow to set custom headers" do
        stub_request(:put, 'https://bucket.s3.amazonaws.com:443/the-key').
          with(:headers => {
            "Authorization"=>"AWS abc:wrPkGKrlwH2AtNzBVS80vU73TDc=",
            'date' => @time,
            'url' => "/bucket/the-key",
            "x-amz-acl" => 'public-read',
            'Cache-Control' => "max-age=252460800",
            'Expires' => 'Fri, 16 Nov 2018 22:09:29 GMT',
            'x-amz-meta-abc' => 'ABC'}).to_return(fake_response("data-here")).times(1)

        @item = Happening::S3::Item.new('bucket', 'the-key', :aws_access_key_id => 'abc',
                                                             :aws_secret_access_key => '123' ,
                                                             :permissions => 'public-read')
        EM.run do
          @item.put('content', :headers => {
             'Expires' => 'Fri, 16 Nov 2018 22:09:29 GMT',
             'Cache-Control' => "max-age=252460800",
             'x-amz-meta-abc' => 'ABC'})

          EM.assertions do
            assert_requested :put, 'https://bucket.s3.amazonaws.com:443/the-key', :times => 1
          end
        end
      end

      should "validate the headers" do
        @item = Happening::S3::Item.new('bucket', 'the-key', :aws_access_key_id => 'abc',
                                                             :aws_secret_access_key => '123' ,
                                                             :permissions => 'public-read')
        assert_raise(ArgumentError) do
          @item.put('content', :headers => {
             'expires' => 'Fri, 16 Nov 2018 22:09:29 GMT',
             'cache_control' => "max-age=252460800"})
        end
      end

      should "re-post to a new location" do
        stub_request(:put, 'https://bucket.s3.amazonaws.com:443/the-key').
          with(:headers => {
            "Authorization"=>"AWS abc:lZMKxGDKcQ1PH8yjbpyN7o2sPWg=",
            'date' => @time,
            'url' => "/bucket/the-key"}).
          to_return(redirect_response('https://bucket.s3-external-3.amazonaws.com/the-key')).times(1)
        stub_request(:put, 'https://bucket.s3-external-3.amazonaws.com:443/the-key').
          with(:headers => {
            "Authorization"=>"AWS abc:lZMKxGDKcQ1PH8yjbpyN7o2sPWg=",
            'date' => @time,
            'url' => "/bucket/the-key"}).to_return(fake_response('Thanks!')).times(1)

        @item = Happening::S3::Item.new('bucket', 'the-key',
          :aws_access_key_id => 'abc',
          :aws_secret_access_key => '123')

        EM.run do
          @item.put('content')

          EM.assertions do
            assert_requested :put, 'https://bucket.s3.amazonaws.com:443/the-key', :times => 1
            assert_requested :put, 'https://bucket.s3-external-3.amazonaws.com:443/the-key', :times => 1
          end
        end
      end

      should "retry on error" do
        stub_request(:put, 'https://bucket.s3.amazonaws.com:443/the-key').
          with(:headers => {
            "Authorization"=>"AWS abc:lZMKxGDKcQ1PH8yjbpyN7o2sPWg=",
            'date' => @time,
            'url' => "/bucket/the-key"}).to_return(error_response(400)).times(5)

        @item = Happening::S3::Item.new('bucket', 'the-key',
          :aws_access_key_id => 'abc',
          :aws_secret_access_key => '123')

        EM.run do
          @item.put('content', :on_error => Proc.new{} )

          EM.assertions do
            assert_requested :put, 'https://bucket.s3.amazonaws.com:443/the-key', :times => 5
          end
        end
      end

      should "call error handler after retry reached" do
        stub_request(:put, 'https://bucket.s3.amazonaws.com:443/the-key').
          with(:headers => {
            "Authorization"=>"AWS abc:lZMKxGDKcQ1PH8yjbpyN7o2sPWg=",
            'date' => @time,
            'url' => "/bucket/the-key"}).to_return(error_response(400)).times(2)


        called = false
        on_error = Proc.new { |http| called = true }

        @item = Happening::S3::Item.new('bucket', 'the-key',
          :aws_access_key_id => 'abc',
          :aws_secret_access_key => '123')

        EM.run do
          @item.put('content', :on_error => on_error, :retry_count => 1)

          EM.assertions do
            assert called
            assert_requested :put, 'https://bucket.s3.amazonaws.com:443/the-key', :times => 2
          end
        end
      end

    end

    context "SSL options" do
      setup do
        Happening::S3.ssl_options[:verify_peer] = true
        Happening::S3.ssl_options[:cert_chain_file] = '/etc/foo.ca'
      end

      should "re-use the global options" do
        item = Happening::S3::Item.new('bucket', 'the-key',
          :aws_access_key_id => 'abc',
          :aws_secret_access_key => '123')
        
        assert item.options[:ssl][:verify_peer]
        assert_equal '/etc/foo.ca', item.options[:ssl][:cert_chain_file]
      end

      should "allow to override global options" do
        item = Happening::S3::Item.new('bucket', 'the-key',
          :aws_access_key_id => 'abc',
          :aws_secret_access_key => '123',
          :ssl => {:cert_chain_file => nil, :verify_peer => false})
        assert !item.options[:ssl][:verify_peer]
        assert_nil item.options[:ssl][:cert_chain_file]
      end

      should "pass the options to the Request" do
        item = Happening::S3::Item.new('bucket', 'the-key',
          :aws_access_key_id => 'abc',
          :aws_secret_access_key => '123')
        Happening::S3::Request.expects(:new).with(:get, anything, {
            :ssl => {:cert_chain_file => '/etc/foo.ca', :verify_peer => true},
            :headers => {
              'Authorization' => 'AWS abc:LGLdCdGTuLAHs+InbMWEnQR6djc=',
              'date' => 'Thu, 25 Feb 2010 10:00:00 GMT'}}).returns(stub(:execute => nil))
        
        item.get
      end

      should "allow to override the options per request" do
        item = Happening::S3::Item.new('bucket', 'the-key', :aws_access_key_id => 'abc', :aws_secret_access_key => '123')
        Happening::S3::Request.expects(:new).with(:get, anything, {
            :ssl => {:foo => :bar},
            :headers => {
              'Authorization' => 'AWS abc:LGLdCdGTuLAHs+InbMWEnQR6djc=',
              'date' => 'Thu, 25 Feb 2010 10:00:00 GMT'}}).returns(stub(:execute => nil))
        
        item.get(:ssl => {:foo => :bar})
      end
    end

  end
end
