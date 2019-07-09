
require 'back_pressure/gated_executor'
require 'timeout'

RSpec.describe(BackPressure::GatedExecutor) do
  subject(:gated_executor) { BackPressure::GatedExecutor.new }

  describe '#execute' do
    it 'yields to the given block' do
      expect { |b| gated_executor.execute(&b) }.to yield_control
    end
    it 'returns true' do
      expect(gated_executor.execute { }).to be true
    end

    context 'when back-pressure is engaged' do
      before(:each) { gated_executor.engage_back_pressure }
      let(:custom_timeout_class) { Class.new(RuntimeError) }

      it 'blocks indefinitely' do
        expect do
          Timeout.timeout(10, custom_timeout_class) do
            gated_executor.execute { fail('Illegal') }
          end
        end.to raise_exception(custom_timeout_class)
      end

      context 'and blocking_limit is given' do
        it 'returns false' do
          expect(gated_executor.execute(1) {}).to be false
        end

        it 'does not yield the block' do
          expect { |b| gated_executor.execute(1, &b) }.to_not yield_control
        end

        it 'returns in a reasonable amount of time' do
          start = Time.now
          gated_executor.execute(1) {}
          duration = Time.now - start

          expect(duration).to be_within(0.1).of(1)
        end

        context 'and back-pressure is subsequently removed' do
          it 'returns true' do
            object = Object.new

            thread = Thread.new { gated_executor.execute(1) { object }}

            sleep 0.5

            gated_executor.remove_back_pressure

            sleep 0.5

            expect(thread.value).to be true
          end

          it 'yields to the block' do
            thread = Thread.new do
              expect { |b| gated_executor.execute(1, &b) }.to yield_control
            end

            sleep 0.5

            gated_executor.remove_back_pressure

            sleep 0.5

            thread.join
          end
        end
      end
    end
  end

  describe '#execute!' do
    it 'yields to the given block' do
      expect { |b| gated_executor.execute!(&b) }.to yield_control

    end
    it 'returns the result of the block' do
      object = Object.new

      expect(gated_executor.execute! { object }).to be object
    end

    context 'when back-pressure is engaged' do
      before(:each) { gated_executor.engage_back_pressure }
      let(:custom_timeout_class) { Class.new(RuntimeError) }

      it 'blocks indefinitely' do
        expect do
          Timeout.timeout(10, custom_timeout_class) do
            gated_executor.execute! { fail('Illegal') }
          end
        end.to raise_exception(custom_timeout_class)
      end

      context 'and blocking_limit is given' do
        it 'raises ExecutionExpired Exception' do
          expect { gated_executor.execute!(1) {} }.to raise_exception(BackPressure::ExecutionExpired)
        end

        it 'does not yield the block' do
          expect { |b| gated_executor.execute!(1, &b) rescue nil }.to_not yield_control
        end

        it 'returns in a reasonable amount of time' do
          start = Time.now
          gated_executor.execute!(1) {} rescue nil
          duration = Time.now - start

          expect(duration).to be_within(0.1).of(1)
        end

        context 'and back-pressure is subsequently removed' do
          it 'returns the result of the block' do
            object = Object.new

            thread = Thread.new { gated_executor.execute!(1) { object } }

            sleep 0.5

            gated_executor.remove_back_pressure

            sleep 0.5

            expect(thread.value).to be object
          end
        end
      end
    end
  end
end