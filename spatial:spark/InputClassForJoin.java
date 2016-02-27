package geospatial.operation;

import java.io.Serializable;

public class InputClassForJoin {
	public static class input1 implements Serializable {
		private String id;
		private double x1;
		private double y1;
		private double x2;
		private double y2;

		public input1(String string, String string2, String string3,
				String string4, String string5) {
			this.id = string;
			this.x1 = Math.max(Double.parseDouble(string2),
					Double.parseDouble(string4));
			this.y1 = Math.max(Double.parseDouble(string3),
					Double.parseDouble(string5));
			this.x2 = Math.min(Double.parseDouble(string2),
					Double.parseDouble(string4));
			this.y2 = Math.min(Double.parseDouble(string3),
					Double.parseDouble(string5));
		}

		public double getx1() {
			return x1;
		}

		public void setx1(double x1) {
			this.x1 = x1;
		}

		public double getx2() {
			return x2;
		}

		public void setx2(double x2) {
			this.x2 = x2;
		}

		public double gety1() {
			return y1;
		}

		public double gety2() {
			return y2;
		}

		public String getid() {
			return id;
		}
	}

	public static class input2 implements Serializable {
		private String id;
		private double x1;
		private double y1;
		private double x2;
		private double y2;

		public input2(String string, String string2, String string3,
				String string4, String string5) {
			this.id = string;
			this.x1 = Double.parseDouble(string2);
			this.y1 = Double.parseDouble(string3);
			this.x2 = Double.parseDouble(string4);
			this.y2 = Double.parseDouble(string5);
		}

		public double getx1() {
			return x1;
		}

		public void setx1(double x1) {
			this.x1 = x1;
		}

		public double getx2() {
			return x2;
		}

		public void setx2(double x2) {
			this.x2 = x2;
		}

		public double gety1() {
			return y1;
		}

		public double gety2() {
			return y2;
		}

		public String getid() {
			return id;
		}

	}
}
