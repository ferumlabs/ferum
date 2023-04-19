import Link from "next/link";

import { useState } from "react";
import Button from "./Button";

export const NavigationLinks = {
  twitter: "https://twitter.com/ferumxyz",
  typeform: "https://survey.typeform.com/to/xVtSzJUI",
  discord: "https://discord.gg/ferum",
  docs: "https://docs.ferum.xyz/",
  github: "https://github.com/ferumlabs/",
};

const CompanyLogo = (props: any) => {
  return (
    <div className="flex items-center justify-left space-x-2 cursor-pointer">
      <svg
        width="158"
        height="66"
        viewBox="0 0 158 66"
        fill="none"
        xmlns="http://www.w3.org/2000/svg"
      >
        <path
          d="M9.65333 50.504V14.7221H30.4673V19.157H14.3403V29.9924H27.0907V34.4273H14.3403V50.504H9.65333ZM44.5867 51.0079C40.9245 51.0079 37.8839 49.7648 35.4649 47.2786C33.0794 44.7923 31.8867 41.6677 31.8867 37.9047C31.8867 34.1081 33.0458 30.9835 35.3641 28.5309C37.7159 26.0446 40.8237 24.8015 44.6875 24.8015C48.5849 24.8015 51.6423 26.179 53.8598 28.9341C56.0772 31.6891 57.0516 35.0657 56.7828 39.0638H36.2712C36.3384 41.3485 37.1784 43.2636 38.7911 44.8091C40.4038 46.321 42.3525 47.077 44.6371 47.077C48.0641 47.077 50.5 45.6659 51.9447 42.8436H56.3796C55.7413 45.1955 54.3805 47.1442 52.2975 48.6897C50.2144 50.2352 47.6441 51.0079 44.5867 51.0079ZM49.5256 30.446C48.1481 29.2364 46.485 28.6317 44.5363 28.6317C42.5876 28.6317 40.8405 29.2196 39.295 30.3956C37.7831 31.5715 36.8424 33.3186 36.4728 35.6369H52.1967C51.7935 33.3522 50.9031 31.6219 49.5256 30.446Z"
          fill="white"
        />
        <path
          d="M68.7199 50V24.8015H73.054V29.69C73.5244 28.1445 74.4315 26.9014 75.7754 25.9606C77.153 25.0199 78.5977 24.5495 80.1096 24.5495C80.8487 24.5495 81.5039 24.6167 82.0751 24.7511V29.2364C81.4703 28.9677 80.6807 28.8333 79.7064 28.8333C77.9593 28.8333 76.4138 29.5892 75.0699 31.1011C73.726 32.613 73.054 34.7465 73.054 37.5015V50H68.7199ZM96.0961 50.504C93.341 50.504 91.1067 49.58 89.3933 47.7321C87.6798 45.8506 86.823 43.3644 86.823 40.2734V24.8015H91.1571V39.6182C91.1571 41.7349 91.6611 43.4148 92.6691 44.6579C93.7106 45.901 95.1049 46.5226 96.852 46.5226C98.8679 46.5226 100.531 45.6995 101.841 44.0532C103.185 42.3733 103.857 40.1222 103.857 37.3V24.8015H108.191V50H103.857V46.1194C102.211 49.0425 99.6238 50.504 96.0961 50.504ZM148.816 50V35.1833C148.816 33.1002 148.38 31.4371 147.506 30.194C146.666 28.9173 145.44 28.2789 143.827 28.2789C141.811 28.2789 140.165 29.0684 138.888 30.6476C137.645 32.2267 136.99 34.3769 136.923 37.0984V50H132.589V35.1833C132.589 33.0666 132.152 31.3867 131.278 30.1436C130.438 28.9005 129.229 28.2789 127.65 28.2789C125.567 28.2789 123.887 29.1188 122.61 30.7987C121.333 32.445 120.695 34.6793 120.695 37.5015V50H116.361V24.8015H120.695V28.6821C122.308 25.759 124.844 24.2975 128.305 24.2975C132.135 24.2975 134.722 26.095 136.066 29.69C136.738 28.0437 137.813 26.7334 139.291 25.759C140.77 24.7847 142.45 24.2975 144.331 24.2975C147.053 24.2975 149.203 25.2215 150.782 27.0694C152.361 28.9173 153.151 31.4035 153.151 34.5281V50H148.816Z"
          fill="white"
        />
        <rect
          x="0.755955"
          y="3.52869"
          width="59.2291"
          height="60.7382"
          stroke="white"
          stroke-width="1.51191"
        />
      </svg>
    </div>
  );
};

const Navigation = () => {
  const [showsMobileMenu, setShowsMobileMenu] = useState<boolean>(false);

  function toggleMobileMenu() {
    setShowsMobileMenu(!showsMobileMenu);
  }

  return (
    <nav className="px-5 sm:px-10 h-20">
      {/* Desktop Nav Bar */}
      <ul className="items-center space-x-8 lg:space-x-8 justify-left hidden lg:flex py-4">
        <li className="grow">
          <Link href="/" passHref>
            <CompanyLogo />
          </Link>
        </li>
        <li>
          <Link href={NavigationLinks.twitter}>
            <a className="nav-link">Twitter</a>
          </Link>
        </li>
        <li>
          <Button title="Join Waitlist" href={NavigationLinks.typeform} />
        </li>
      </ul>

      {/* Mobile Nav Bar */}
      <ul className="mobile flex items-center justify-end lg:hidden py-4">
        <li className="grow">
          <Link href="/" passHref>
            <div className="flex items-center justify-left space-x-2">
              <CompanyLogo dimension="40" id="mobile" />
            </div>
          </Link>
        </li>
        <li>
          <button onClick={toggleMobileMenu}>
            <svg
              xmlns="http://www.w3.org/2000/svg"
              fill="none"
              viewBox="0 0 24 24"
              stroke="white"
              aria-hidden="true"
              className="w-8 h-8"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth="2"
                d="M4 6h16M4 12h16M4 18h16"
              ></path>
            </svg>
          </button>
        </li>
      </ul>

      {/* Mobile Menu */}
      {showsMobileMenu && (
        <div className="fixed inset-0 bg-inherit z-10 px-5">
          <ul className="flex items-center justify-end py-4 ">
            <li className="grow">
              <Link href="/">
                <a className="hover:opacity-40">Ferum</a>
              </Link>
            </li>
            <li>
              <button onClick={toggleMobileMenu}>
                <svg
                  xmlns="http://www.w3.org/2000/svg"
                  fill="none"
                  viewBox="0 0 24 24"
                  stroke="white"
                  aria-hidden="true"
                  className="w-8 h-8"
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth="2"
                    d="M6 18L18 6M6 6l12 12"
                  ></path>
                </svg>
              </button>
            </li>
          </ul>

          <ul className="flex-row space-y-2 items-center py-2">
            <li>
              <Link href={NavigationLinks.twitter}>
                <a className="hover:opacity-40" onClick={toggleMobileMenu}>
                  Twitter
                </a>
              </Link>
            </li>
          </ul>
        </div>
      )}
    </nav>
  );
};

Navigation.defaultProps = {};

export default Navigation;
