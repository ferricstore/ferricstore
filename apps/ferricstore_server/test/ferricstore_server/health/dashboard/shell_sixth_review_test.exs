defmodule FerricstoreServer.Health.Dashboard.ShellSixthReviewTest do
  use FerricstoreServer.Test.DashboardCase

  alias FerricstoreServer.Health.Dashboard.Render.Admin
  alias FerricstoreServer.Health.Dashboard.Render.TableValue

  test "long values have bounded stable disclosure keys and escaped content" do
    value = String.duplicate("<script>\"&", 100)
    html = TableValue.render(value, "payload")
    assert key(html) == key(TableValue.render(value, "payload"))
    assert byte_size(key(html)) == 43
    assert html =~ "&lt;script&gt;"
    refute html =~ "<script>"
  end

  test "record identities separate duplicate values and retain changes to the same record" do
    value = String.duplicate("same command ", 8)
    first = TableValue.render(value, "command", nil, 1)
    second = TableValue.render(value, "command", nil, 2)
    updated = TableValue.render(value <> "updated", "command", nil, 1)
    refute key(first) == key(second)
    assert key(first) == key(updated)
    refute key(first) == key(TableValue.render(value, "other field", nil, 1))
  end

  test "slowlog uses entry identity even when different rows contain identical commands" do
    entries =
      for id <- [1, 2],
          do: %{
            id: id,
            command: [String.duplicate("command", 10)],
            duration_us: 5,
            timestamp_us: 1_700_000_000_000_000
          }

    keys = keys(Admin.render_slowlog_table(entries))
    assert length(Enum.uniq(keys)) == 2
    assert keys(Admin.render_slowlog_table(Enum.reverse(entries))) == Enum.reverse(keys)
  end

  defp key(html), do: hd(keys(html))

  defp keys(html),
    do:
      Regex.scan(~r/data-dashboard-disclosure-key="([^"]+)"/, html, capture: :all_but_first)
      |> List.flatten()
end
