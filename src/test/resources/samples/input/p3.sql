SELECT * FROM Sailors WHERE Sailors.A >= 100 and Sailors.A < 150 AND Sailors.B > 1000;
SELECT * FROM Sailors S WHERE S.A >= 100 and S.A < 150;
SELECT * FROM Reserves R WHERE R.G < 100;
SELECT * FROM Sailors S, Reserves R WHERE S.B = R.G AND R.H < 50;
SELECT * FROM Sailors S, Reserves R WHERE S.B = R.G AND R.H < 50 AND S.A >= 9050;