defmodule Ferricstore.Store.PromotionTest.Sections.SmallSortedSetStaysInSharedBitcask do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      import ExUnit.CaptureLog
      alias Ferricstore.Commands.{Hash, List, Set, SortedSet, Strings}
      alias Ferricstore.Bitcask.NIF
      alias Ferricstore.HLC
      alias Ferricstore.Store.{CompoundKey, Promotion, Router}
      alias Ferricstore.Store.Shard.Compound, as: ShardCompound
      alias Ferricstore.Test.ShardHelpers

      describe "small sorted set stays in shared Bitcask" do
        test "zset with fewer members than threshold is not promoted" do
          store = real_store()
          key = ukey("small_zset")

          populate_zset(store, key, @test_threshold - 1)

          refute promoted?(key)
          assert @test_threshold - 1 == SortedSet.handle("ZCARD", [key], store)
        end

        test "zset with exactly threshold members is not promoted (threshold is exclusive)" do
          store = real_store()
          key = ukey("exact_threshold_zset")

          populate_zset(store, key, @test_threshold)

          refute promoted?(key)
        end
      end

      # ---------------------------------------------------------------------------
      # Sorted set exceeding threshold gets promoted
      # ---------------------------------------------------------------------------

      describe "sorted set promotion on threshold crossing" do
        test "zset crossing threshold gets promoted to dedicated Bitcask" do
          store = real_store()
          key = ukey("promote_zset")

          # Insert members up to threshold (not yet promoted)
          populate_zset(store, key, @test_threshold)
          refute promoted?(key)

          # Add one more member to cross the threshold
          SortedSet.handle("ZADD", [key, "999.0", "extra_member"], store)

          assert promoted?(key)
        end

        test "promoted zset has dedicated directory on disk" do
          store = real_store()
          key = ukey("promote_zset_dir")

          populate_zset(store, key, @test_threshold + 1)

          assert promoted?(key)

          # Verify the dedicated directory exists
          data_dir = Application.fetch_env!(:ferricstore, :data_dir)
          shard_idx = Router.shard_for(FerricStore.Instance.get(:default), key)
          hash = :crypto.hash(:sha256, key) |> Base.encode16(case: :lower)

          dedicated_path =
            Path.join([data_dir, "dedicated", "shard_#{shard_idx}", "zset:#{hash}"])

          assert File.dir?(dedicated_path)
        end
      end

      # ---------------------------------------------------------------------------
      # ZRANGE on promoted sorted set
      # ---------------------------------------------------------------------------

      describe "ZRANGE on promoted sorted set" do
        test "ZRANGE returns all members in order after promotion" do
          store = real_store()
          key = ukey("zrange_promoted")
          n = @test_threshold + 1

          populate_zset(store, key, n)
          assert promoted?(key)

          members = SortedSet.handle("ZRANGE", [key, "0", "-1"], store)
          assert length(members) == n

          # Members should be in score order (1.0, 2.0, ..., n.0)
          expected = for i <- 1..n, do: "member_#{i}"
          assert members == expected
        end

        test "ZRANGE WITHSCORES returns members and scores after promotion" do
          store = real_store()
          key = ukey("zrange_ws_promoted")
          n = @test_threshold + 1

          populate_zset(store, key, n)
          assert promoted?(key)

          result = SortedSet.handle("ZRANGE", [key, "0", "-1", "WITHSCORES"], store)
          # Result is [member1, score1, member2, score2, ...]
          assert length(result) == n * 2

          pairs = Enum.chunk_every(result, 2)
          first_pair = hd(pairs)
          assert first_pair == ["member_1", "1.0"]
        end
      end

      # ---------------------------------------------------------------------------
      # ZSCORE on promoted sorted set
      # ---------------------------------------------------------------------------

      describe "ZSCORE on promoted sorted set" do
        test "ZSCORE returns correct score after promotion" do
          store = real_store()
          key = ukey("zscore_promoted")

          populate_zset(store, key, @test_threshold + 1)
          assert promoted?(key)

          score = SortedSet.handle("ZSCORE", [key, "member_3"], store)
          assert score == "3.0"
        end

        test "ZSCORE returns nil for missing member in promoted zset" do
          store = real_store()
          key = ukey("zscore_miss_promoted")

          populate_zset(store, key, @test_threshold + 1)
          assert promoted?(key)

          assert nil == SortedSet.handle("ZSCORE", [key, "nonexistent"], store)
        end
      end

      # ---------------------------------------------------------------------------
      # ZREM on promoted sorted set
      # ---------------------------------------------------------------------------

      describe "ZREM on promoted sorted set" do
        test "ZREM removes member from promoted zset" do
          store = real_store()
          key = ukey("zrem_promoted")

          populate_zset(store, key, @test_threshold + 1)
          assert promoted?(key)

          assert 1 == SortedSet.handle("ZREM", [key, "member_1"], store)
          assert nil == SortedSet.handle("ZSCORE", [key, "member_1"], store)
        end

        test "ZREM on missing member in promoted zset returns 0" do
          store = real_store()
          key = ukey("zrem_miss_promoted")

          populate_zset(store, key, @test_threshold + 1)
          assert promoted?(key)

          assert 0 == SortedSet.handle("ZREM", [key, "nonexistent"], store)
        end
      end

      # ---------------------------------------------------------------------------
      # ZCARD on promoted sorted set
      # ---------------------------------------------------------------------------

      describe "ZCARD on promoted sorted set" do
        test "ZCARD returns correct count for promoted zset" do
          store = real_store()
          key = ukey("zcard_promoted")
          n = @test_threshold + 1

          populate_zset(store, key, n)
          assert promoted?(key)

          assert n == SortedSet.handle("ZCARD", [key], store)
        end

        test "ZCARD updates after member addition on promoted zset" do
          store = real_store()
          key = ukey("zcard_add_promoted")
          n = @test_threshold + 1

          populate_zset(store, key, n)
          assert promoted?(key)

          SortedSet.handle("ZADD", [key, "999.0", "extra"], store)
          assert n + 1 == SortedSet.handle("ZCARD", [key], store)
        end

        test "ZCARD updates after member removal on promoted zset" do
          store = real_store()
          key = ukey("zcard_rem_promoted")
          n = @test_threshold + 1

          populate_zset(store, key, n)
          assert promoted?(key)

          SortedSet.handle("ZREM", [key, "member_1"], store)
          assert n - 1 == SortedSet.handle("ZCARD", [key], store)
        end
      end

      # ---------------------------------------------------------------------------
      # ZADD on promoted sorted set (adding after promotion)
      # ---------------------------------------------------------------------------

      describe "ZADD on promoted sorted set" do
        test "ZADD adds new member to promoted zset" do
          store = real_store()
          key = ukey("zadd_promoted")

          populate_zset(store, key, @test_threshold + 1)
          assert promoted?(key)

          assert 1 == SortedSet.handle("ZADD", [key, "42.5", "new_member"], store)
          assert "42.5" == SortedSet.handle("ZSCORE", [key, "new_member"], store)
        end

        test "ZADD updates score of existing member in promoted zset" do
          store = real_store()
          key = ukey("zadd_update_promoted")

          populate_zset(store, key, @test_threshold + 1)
          assert promoted?(key)

          assert 0 == SortedSet.handle("ZADD", [key, "99.9", "member_1"], store)
          score = SortedSet.handle("ZSCORE", [key, "member_1"], store)
          # Float representation may vary slightly (e.g. "99.9" vs "99.90000000000000576")
          assert_in_delta String.to_float(score), 99.9, 0.001
        end
      end

      # ---------------------------------------------------------------------------
      # ZRANK on promoted sorted set
      # ---------------------------------------------------------------------------

      describe "ZRANK on promoted sorted set" do
        test "ZRANK returns correct rank after promotion" do
          store = real_store()
          key = ukey("zrank_promoted")

          populate_zset(store, key, @test_threshold + 1)
          assert promoted?(key)

          # member_1 has score 1.0, should be rank 0 (lowest)
          assert 0 == SortedSet.handle("ZRANK", [key, "member_1"], store)
        end
      end

      # ---------------------------------------------------------------------------
      # DEL on promoted sorted set cleans up dedicated instance
      # ---------------------------------------------------------------------------

      describe "DEL on promoted sorted set" do
        test "DEL removes promoted zset and cleans up dedicated Bitcask" do
          store = real_store()
          key = ukey("del_promoted_zset")

          populate_zset(store, key, @test_threshold + 1)
          assert promoted?(key)

          # DEL the key
          Strings.handle("DEL", [key], store)

          # Key should be gone
          refute promoted?(key)
          assert nil == SortedSet.handle("ZSCORE", [key, "member_1"], store)
          assert 0 == SortedSet.handle("ZCARD", [key], store)

          # Verify the dedicated directory was cleaned up
          data_dir = Application.fetch_env!(:ferricstore, :data_dir)
          shard_idx = Router.shard_for(FerricStore.Instance.get(:default), key)
          hash = :crypto.hash(:sha256, key) |> Base.encode16(case: :lower)

          dedicated_path =
            Path.join([data_dir, "dedicated", "shard_#{shard_idx}", "zset:#{hash}"])

          refute File.dir?(dedicated_path)
        end
      end

      # ---------------------------------------------------------------------------
      # Sorted set promotion is one-way
      # ---------------------------------------------------------------------------

      describe "sorted set promotion is one-way" do
        test "zset stays promoted even with few members remaining" do
          store = real_store()
          key = ukey("stays_promoted_zset")
          n = @test_threshold + 1

          populate_zset(store, key, n)
          assert promoted?(key)

          # Delete most members, keep 2
          for i <- 3..n do
            SortedSet.handle("ZREM", [key, "member_#{i}"], store)
          end

          assert promoted?(key)
          assert 2 == SortedSet.handle("ZCARD", [key], store)
          assert "1.0" == SortedSet.handle("ZSCORE", [key, "member_1"], store)
        end
      end

      # ===========================================================================
      # LIST NON-PROMOTION
      #
      # Lists store all elements as a single serialized Erlang term in one
      # Bitcask entry (via ListOps). They do NOT use compound keys, so the
      # promotion system does not apply. A list with 1000 elements is still
      # one Bitcask entry, not 1000. Promotion is intentionally skipped for
      # lists because there is no compound-key fan-out to consolidate.
      # ===========================================================================

      describe "lists are NOT promoted" do
        test "list with more elements than threshold is not promoted" do
          store = real_store()
          key = ukey("big_list")

          # Push many more elements than the threshold
          elements = Enum.map(1..(@test_threshold * 3), fn i -> "elem_#{i}" end)
          List.handle("RPUSH", [key | elements], store)

          refute promoted?(key)

          # All elements still accessible
          assert @test_threshold * 3 == List.handle("LLEN", [key], store)
        end

        test "list operations work normally without promotion" do
          store = real_store()
          key = ukey("list_no_promo")

          elements = Enum.map(1..(@test_threshold + 5), fn i -> "elem_#{i}" end)
          List.handle("RPUSH", [key | elements], store)

          refute promoted?(key)

          # Verify basic list operations
          assert "elem_1" == List.handle("LINDEX", [key, "0"], store)
          assert "elem_#{@test_threshold + 5}" == List.handle("LINDEX", [key, "-1"], store)

          # LPOP and RPOP work
          assert "elem_1" == List.handle("LPOP", [key], store)
          assert "elem_#{@test_threshold + 5}" == List.handle("RPOP", [key], store)
          assert @test_threshold + 3 == List.handle("LLEN", [key], store)

          # Still not promoted after all operations
          refute promoted?(key)
        end
      end
    end
  end
end
