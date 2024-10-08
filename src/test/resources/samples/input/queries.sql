SELECT * FROM Sailors;
SELECT Sailors.A FROM Sailors;
SELECT S.A FROM Sailors S;
SELECT * FROM Sailors S WHERE S.A < 3;
SELECT * FROM Sailors, Reserves WHERE Sailors.A = Reserves.G;
SELECT * FROM Sailors S1, Sailors S2 WHERE S1.A < S2.A;
SELECT DISTINCT R.G FROM Reserves R;
SELECT * FROM Sailors ORDER BY Sailors.B;
SELECT * FROM Sailors WHERE 1 = 2;
SELECT * FROM Sailors WHERE 1 = 1;
SELECT DISTINCT * FROM Test ORDER BY Test.J;
SELECT Sailors.A FROM Sailors;
SELECT Boats.F, Boats.D FROM Boats;
SELECT Reserves.G, Reserves.H FROM Reserves;
SELECT * FROM Sailors WHERE Sailors.B >= Sailors.C;
SELECT Sailors.A FROM Sailors WHERE Sailors.B >= Sailors.C;
SELECT Sailors.A FROM Sailors WHERE Sailors.B >= Sailors.C AND Sailors.B < Sailors.C;
SELECT * FROM Sailors, Reserves WHERE Sailors.A = Reserves.G;
SELECT * FROM Sailors, Reserves, Boats WHERE Sailors.A = Reserves.G AND Reserves.H = Boats.D;
SELECT * FROM Sailors, Reserves, Boats WHERE Sailors.A = Reserves.G AND Reserves.H = Boats.D AND Sailors.B < 150;
SELECT DISTINCT * FROM Sailors;
SELECT * FROM Sailors S1, Sailors S2 WHERE S1.A < S2.A;
SELECT B.F, B.D FROM Boats B ORDER BY B.D;
SELECT * FROM Sailors S, Reserves R, Boats B WHERE S.A = R.G AND R.H = B.D ORDER BY S.C;
SELECT DISTINCT * FROM Sailors S, Reserves R, Boats B WHERE S.A = R.G AND R.H = B.D ORDER BY S.C;
SELECT * FROM Sailors S WHERE S.B<=200 ORDER BY S.C, S.A;
SELECT * FROM Sailors S, Reserves R, Boats B WHERE S.A = R.G AND R.H = B.D AND S.B > 150;
SELECT DISTINCT S1.A, S2.A FROM Sailors S1, Sailors S2 WHERE S1.B > S2.B AND S1.A != S2.A;
SELECT S.A, R.G FROM Sailors S, Reserves R WHERE S.A = R.G AND S.B < 200;
SELECT S.A, R.G FROM Sailors S, Reserves R WHERE S.A = R.G ORDER BY S.B DESC;
SELECT DISTINCT S.A FROM Sailors S, Reserves R WHERE S.A = R.G;
