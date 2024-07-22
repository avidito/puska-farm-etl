INSERT INTO distribusi_susu (
  tgl_distribusi,
  id_unit_ternak,
  id_jenis_produk,
  id_mitra_bisnis,
  jumlah,
  harga_berlaku
)
VALUES
  ('2024-05-10', 2, 3, 1, 2, 10000);

UPDATE distribusi_susu
SET
  jumlah = 5,
  harga_berlaku = 20000
WHERE id = ?;

DELETE distribusi_susu
WHERE id = ?;
