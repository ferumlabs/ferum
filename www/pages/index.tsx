import type { NextPage } from "next";
import Container from "../components/Container";
import Button from "../components/Button";
import { NavigationLinks } from "../components/Navigation";

const Home: NextPage = () => {
  return (
    <Container showsNavigation={true}>
      <div className="flex flex-col items-center justify-center min-h-[75vh] w-full lg:py-10 space-y-5">
          <h1 className="lg:text-8xl text-4xl  text-white text-center font-bold ">
            First Decentralized Exchange on Aptos
          </h1>
          <p className="lg:text-2xl text-base opacity-90 leading-normal text-center">
            Harness the power of DeFi. Built on Move. 
          </p>
          <div>
            <Button title="Join Waitlist" href={NavigationLinks.typeform}/>
          </div>
        </div>
    </Container>
  );
};

export default Home;
